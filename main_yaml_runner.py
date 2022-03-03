from api_key_reconciliation import find_api_keys_eligible_for_deletion
import argparse

import yaml

import yaml_parser
from api_key_manager import CCloudAPIKeyList
from base_ccloud import CCloudConnection
from clusters import CCloudClusterList, CCloudEnvironmentList
from config_store import CSMConfigDataMap, CSMConfigObjectType, CSMConfigTask, CSMConfigTaskStatus, CSMConfigTaskType
from service_account import CCloudServiceAccountList


def printline():
    print("=" * 80)


def create_definitions_file(def_file_path: str, sa_details: CCloudServiceAccountList):
    output = {"service_accounts": []}
    for item in sa_details.sa.values():
        acc = {
            "name": item.name,
            "description": item.description,
            "enable_rest_proxy_access": False,
            "team_email_address": "abc@abc.com",
            "api_key_access": [],
        }
        output["service_accounts"].append(acc)
    with open(def_file_path, "w") as f:
        yaml.dump(output, f, sort_keys=False)


def service_account_manager_workflow(k: int, v: CSMConfigTask):
    # If task is Create Service Account
    if v.task_type == CSMConfigTaskType.create_task:
        print("Executing Create SA flow for " + v.task_object["sa_name"])
        csm_conf_store.set_task_status(k, CSMConfigTaskStatus.sts_in_progress, "SA Creation in progress.")
        new_sa, is_created = ccloud_sa_list.create_sa(v.task_object["sa_name"], v.task_object["description"])
        csm_conf_store.set_task_status(
            k,
            CSMConfigTaskStatus.sts_success,
            "SA Creation Succeeded.",
            {"sa_id": new_sa.resource_id, "sa_name": new_sa.name},
        )
    # If task is Delete Service accounts
    elif v.task_type == CSMConfigTaskType.delete_task:
        print("Inside Delete SA flow for " + v.task_object["sa_name"])
        csm_conf_store.set_task_status(k, CSMConfigTaskStatus.sts_in_progress, "SA deletion in progress.")
        sa_id = ccloud_sa_list.find_sa(v.task_object["sa_name"]).resource_id
        is_deleted = ccloud_sa_list.delete_sa(v.task_object["sa_name"])
        csm_conf_store.set_task_status(
            k,
            CSMConfigTaskStatus.sts_success,
            "SA deletion succeeded.",
            {"sa_id": sa_id, "sa_name": v.task_object["sa_name"]},
        )


def api_key_manager_workflow(k: int, v: CSMConfigTask):
    # If task is to create new API Keys
    #  No task exists for deleting API Keys currently
    if v.task_type == CSMConfigTaskType.create_task:
        print(
            "Creating API Key flow for Service Account "
            + v.task_object["sa_name"]
            + " for cluster "
            + v.task_object["cluster_id"]
        )
        csm_conf_store.set_task_status(k, CSMConfigTaskStatus.sts_in_progress, "API Key creation in progress.")
        sa_details = ccloud_sa_list.find_sa(v.task_object["sa_name"])
        new_api_key = ccloud_api_key_list.create_api_key(
            v.task_object["env_id"],
            v.task_object["cluster_id"],
            sa_details.resource_id,
            sa_details.name,
            "API Key for sa " + sa_details.resource_id + " created by the CI/CD workflow",
        )
        csm_conf_store.set_task_status(
            k,
            CSMConfigTaskStatus.sts_success,
            "API Key creation succeeded.",
            {"api_key": new_api_key["key"], "env_id": v.task_object["env_id"]},
        )


def secrets_workflow_manager(k: int, v: CSMConfigTask):
    if v.task_type == CSMConfigTaskType.create_task:
        print(
            "Storing the newly created API Key/Secret in Secret Store for Service Account: " + v.task_object["sa_name"]
        )
        csm_conf_store.set_task_status(k, CSMConfigTaskStatus.sts_in_progress, "Secret Store Addition in progress.")
        sa_details = ccloud_sa_list.find_sa(v.task_object["sa_name"])
        api_key_details = ccloud_api_key_list.find_keys_with_sa_and_cluster(
            sa_details.resource_id, v.task_object["cluster_id"]
        )
        for item in api_key_details:
            if item.api_secret:
                secret_list.create_or_update_secret(
                    item, ccloud_env_list, ccloud_cluster_list, ccloud_sa_list, csm_definitions, csm_configs
                )


def run_workflow():
    secrets_delta = False
    # Iterate on all the tasks created by the plan
    for k, v in csm_conf_store.select_new_tasks():
        # If the tasks are related to Service Accounts
        if v.object_type == CSMConfigObjectType.sa_type:
            service_account_manager_workflow(k, v)
        # If the task is related to API Keys
        elif v.object_type == CSMConfigObjectType.api_key_type:
            api_key_manager_workflow(k, v)
        elif v.object_type == CSMConfigObjectType.secret_store_type:
            secrets_workflow_manager(k, v)
            secrets_delta = True
    if secrets_delta:
        secret_list.create_update_rest_proxy_secrets(
            csm_definitions, csm_configs, ccloud_api_key_list, ccloud_cluster_list, ccloud_env_list, ccloud_sa_list
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application",
        add_help=True,
    )

    conf_args = parser.add_argument_group(
        "configuration-args", "Configuration Arguments for running in CI/CD flow invocation"
    )
    conf_args.add_argument(
        "--csm-config-file-path",
        type=str,
        default=None,
        metavar="/full/path/of/the/configuration/file.yaml",
        help="This is the configuration file path that will provide the connectivity and other config details.",
    )
    conf_args.add_argument(
        "--csm-definitions-file-path",
        type=str,
        default=None,
        metavar="/full/path/of/the/definitions/file.yaml",
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )
    conf_args.add_argument(
        "--csm-generate-definitions-file",
        default=False,
        action="store_true",
        help="This switch can be used for the initial runs where the team does not have a definitions file and would like to auto generate one from the existing ccloud resource mappings.",
    )
    conf_args.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="This switch can be used to invoke a dry run and list all the action that will be preformed, but not performing them.",
    )
    conf_args.add_argument(
        "--disable-api-key-creation",
        default=False,
        action="store_true",
        help="This switch can be used to disable API Key & Secret creation (if required)",
    )
    conf_args.add_argument(
        "--print-delete-eligible-api-keys",
        default=False,
        action="store_true",
        help="This switch can be used to print the API keys which are not synced to the Secret store and (potentially) not used.",
    )

    args = parser.parse_args()

    printline()
    # parse the YAML files for the input configurations
    csm_configs, csm_definitions = yaml_parser.load_parse_yamls(
        args.csm_config_file_path, args.csm_definitions_file_path, args.csm_generate_definitions_file
    )
    printline()
    # Initialize CCloud Connection Details
    ccloud_conn = CCloudConnection(csm_configs)
    printline()
    # Gather Environment List from CCloud
    ccloud_env_list = CCloudEnvironmentList(ccloud_conn)
    printline()
    # Gather Cluster ist from al the environments
    ccloud_cluster_list = CCloudClusterList(ccloud_conn, ccloud_env_list)
    printline()
    # Gather Service Account details pre-existing in CCloud
    ccloud_sa_list = CCloudServiceAccountList(ccloud_conn, csm_configs)
    printline()
    print("List of ignored accounts: ")
    for item in csm_configs.ccloud.ignore_service_account_list:
        sa_details = ccloud_sa_list.sa.get(item, None)
        if sa_details:
            print("SA ID: " + sa_details.resource_id + "\t\t SA Name: " + sa_details.name)
    printline()
    ccloud_api_key_list = CCloudAPIKeyList(ccloud_sa_list)
    printline()
    # If the Generate YAML is True, we will parse the data and render a YAML file
    if args.csm_generate_definitions_file:
        create_definitions_file("test_output.yaml", ccloud_sa_list)
    # This path will only get executed if the YAML files is passed in and
    # Generate YAML file is unchecked.
    else:
        if csm_configs.secretstore.type == yaml_parser.SUPPORTED_STORES.AWS_SECRETS:
            import aws_secrets_manager

            secret_list = aws_secrets_manager.AWSSecretsList()
            secret_list.read_all_secrets(
                {
                    "secret_manager": [
                        "confluent_cloud",
                    ],
                }
            )
        # Compare and generate the plan for execution
        csm_conf_store = CSMConfigDataMap()
        csm_conf_store.populate_data_map(
            csm_definitions,
            ccloud_sa_list,
            ccloud_api_key_list,
            ccloud_cluster_list,
            csm_configs,
            secret_list,
            args.disable_api_key_creation,
        )
        printline()
        print("Execution Plan")
        csm_conf_store.print_data_map(include_delete=True if csm_configs.ccloud.enable_sa_cleanup else False)
        printline()
        # If Dry run is disabled, plan will be executed.
        if not args.dry_run:
            run_workflow()
        #  Dry Run
        else:
            print("Dry Run was selected. Plan will not be executed")
        printline()

        if args.print_delete_eligible_api_keys:
            key_list = find_api_keys_eligible_for_deletion(
                secret_list,
                ccloud_api_key_list,
                csm_configs.ccloud.ignore_service_account_list,
            )
            if key_list:
                print("Found API Keys that are missing in the secret store but are currently (still) active in CCloud")
                ccloud_api_key_list.print_api_keys(ccloud_sa_list, key_list)
            else:
                print("No deletion eligible keys detected.")

    printline()
