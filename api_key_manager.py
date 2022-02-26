import pprint
import subprocess
from datetime import datetime
from json import dumps, loads
from operator import itemgetter
from typing import Dict, List

import base_ccloud
import clusters
import service_account

pp = pprint.PrettyPrinter(indent=2)


class CCloudAPIKey:
    def __init__(
        self, api_key: str, api_secret: str, api_key_description: str, owner_id: str, cluster_id: str, created_at
    ) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_key_description = api_key_description
        self.owner_id = owner_id
        self.cluster_id = cluster_id
        self.created_at = created_at


class CCloudAPIKeyList:
    api_keys: Dict[str, CCloudAPIKey]
    __CMD_STDERR_TO_STDOUT = " 2>&1 "

    # This init function will initiate the base object and then check CCloud
    # for all the active API Keys. All API Keys that are listed in CCloud are
    # the added to a cache.
    def __init__(self, sa_id_list: service_account.CCloudServiceAccountList) -> None:
        self.api_keys = {}
        print("Gathering list of all API Key(s) for all Service Account(s) in CCloud.")
        self.__read_all_api_keys(sa_id_list)

    # This is the base function that will call the command line tool. The command to be
    # executed is passed in as the command parameter.
    def __execute_subcommand(self, command):
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        out = process.communicate()[0].strip()
        return out.decode("UTF-8")

    # This function can be used to login into the CCloud CLI.
    def __confluent_cli_login(self):
        cmd_login = "confluent login" + self.__CMD_STDERR_TO_STDOUT
        output = self.__execute_subcommand(cmd_login)
        if output != "" and not output.startswith("A minor version update is available"):
            raise Exception(
                "Could not login into Confluent Cloud CLI. Please ensure that the credentials are correct." + output
            )

    # This function should be called after CCloud Login is done and
    # will set the environment passed in as the env_id
    def __confluent_cli_set_env(self, env_id):
        cmd_set_env = "confluent environment use " + env_id
        output = self.__execute_subcommand(cmd_set_env)
        if not output.startswith('Now using "' + env_id + '" as the default (active) environment.'):
            raise Exception(
                "Could not set the current environment to "
                + env_id
                + ". Please fix all the issues before trying again. "
                + output
            )

    def __confluent_cli_set_cluster(self, cluster_id):
        cmd_set_cluster = "confluent kafka cluster use " + cluster_id + self.__CMD_STDERR_TO_STDOUT
        output = self.__execute_subcommand(cmd_set_cluster)
        if not output.startswith('Set Kafka cluster "' + cluster_id + '"'):
            raise Exception(
                "Could not set the current cluster to "
                + cluster_id
                + ". Please fix al the issues before trying again. "
                + output
            )

    # This method will help reading all the API Keys that are already provisioned.
    # Please note that the API Secrets cannot be read back again, so if you do not have
    # access to the secret , you will need to generate new api key/secret pair.
    def __read_all_api_keys(self, sa_id_list: service_account.CCloudServiceAccountList):
        self.__confluent_cli_login()
        print("Gathering all API Keys.")
        cmd_api_key_list = "confluent api-key list -o json "
        output = loads(self.__execute_subcommand(cmd_api_key_list))
        output = sorted(output, key=itemgetter("created"), reverse=True)
        sa_list = [item.resource_id for item in sa_id_list.sa.values()]
        for key in output:
            if key["owner_resource_id"] in sa_list and key["resource_type"] == "kafka" and key["resource_id"]:
                print("Found API Key with ID " + key["key"] + " for Service Account " + key["owner_resource_id"])
                self.__add_to_cache(
                    key["key"],
                    "",
                    key["description"],
                    key["owner_resource_id"],
                    key["resource_id"],
                    key["created"],
                )

    def __add_to_cache(
        self, api_key: str, api_secret: str, api_key_description: str, owner_id: str, cluster_id: str, created_at
    ) -> CCloudAPIKey:
        self.api_keys[api_key] = CCloudAPIKey(
            api_key, api_secret, api_key_description, owner_id, cluster_id, created_at
        )
        return self.api_keys[api_key]

    def delete_keys_from_cache(self, sa_name) -> int:
        count = 0
        for item in self.api_keys.values():
            if sa_name == item.owner_id:
                self.api_keys.pop(item.api_key, None)
                count += 1
        return count

    def __delete_key_from_cache(self, key_id: str) -> int:
        self.api_keys.pop(key_id, None)

    def find_keys_with_sa(self, sa_id: str) -> List[CCloudAPIKey]:
        output = []
        for item in self.api_keys.values():
            if sa_id == item.owner_id:
                output.append(item)
        return output

    def find_keys_with_sa_and_cluster(self, sa_id: str, cluster_id: str) -> List[CCloudAPIKey]:
        output = []
        for item in self.api_keys.values():
            if cluster_id == item.cluster_id and sa_id == item.owner_id:
                output.append(item)
        return output

    def create_api_key(self, env_id: str, cluster_id: str, sa_id: str, sa_name: str, description: str = None):
        self.__confluent_cli_set_env(env_id)
        self.__confluent_cli_set_cluster(cluster_id)
        api_key_description = (
            "API Key for " + sa_name + " created by CI/CD framework." if not description else description
        )
        cmd_create_api_key = (
            "confluent api-key create -o json --service-account "
            + sa_id
            + " --resource "
            + cluster_id
            + ' --description "'
            + api_key_description
            + '"'
            + self.__CMD_STDERR_TO_STDOUT
        )
        output = loads(self.__execute_subcommand(cmd_create_api_key))
        self.__add_to_cache(
            output["key"], output["secret"], api_key_description, sa_id, cluster_id, str(datetime.now())
        )
        return output

    def delete_api_key(self, api_key: str) -> bool:
        cmd_delete_api_key = "confluent api-key delete " + api_key
        output = self.__execute_subcommand(cmd_delete_api_key)
        if not output.startswith("Deleted API key "):
            raise Exception("Could not delete the API Key.")
        else:
            self.__delete_key_from_cache(api_key)
        return True

    def print_api_keys(
        self, ccloud_sa_list: service_account.CCloudServiceAccountList, key_list: List[CCloudAPIKey] = None
    ):
        print(
            "{:<20} {:<25} {:<25} {:<20} {:<20} {:<50}".format(
                "API Key",
                "API Key Cluster ID",
                "Created",
                "API Key Owner ID",
                "API Key Owner Name",
                "API Key Description",
            )
        )
        if key_list:
            iter_data = key_list
        else:
            iter_data = [v for v in self.api_keys.values()]
        for item in iter_data:
            sa_details = ccloud_sa_list.sa[item.owner_id]
            print(
                "{:<20} {:<25} {:<25} {:<20} {:<20} {:<50}".format(
                    item.api_key,
                    item.cluster_id,
                    item.created_at,
                    item.owner_id,
                    sa_details.name,
                    item.api_key_description,
                )
            )


def int_check_setup_api_key(sa_id, sa_name, cluster: dict, force_new_key: bool):
    cluster_id = cluster["id"]
    cluster_name = cluster["spec"]["display_name"]
    env_id = cluster["spec"]["environment"]["id"]
    env_name = clusters.ENV_VALUES[env_id]["display_name"]
    int_confluent_cli_set_env(env_id)
    int_confluent_cli_set_cluster(cluster_id)
    int_confluent_cli_get_api_key_list(sa_id)
    api_key = int_check_existing_api_key(cluster_id, sa_id)
    if api_key:
        int_print_api_key(sa_name, api_key)
    if force_new_key:
        print("Override provided via --force-new-api-keys switch. Generating new API Keys.")
        api_key = None
    if not api_key:
        create_output = int_create_api_key(cluster_id, sa_id, sa_name)
        int_confluent_cli_get_api_key_list(sa_id)
        key_id = create_output["key"]
        api_key = int_check_existing_api_key(cluster_id, sa_id, key_id)
        int_print_api_key(sa_name, api_key)
        OUTPUT_NEWLY_CREATED_KEYS[key_id] = api_key[0].copy()
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["secret"] = create_output["secret"]
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["env_id"] = env_id
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["env_name"] = env_name
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["cluster_name"] = cluster_name
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["sa_name"] = sa_name
    global OUTPUT_API_KEYS
    api_key = int_check_existing_api_key(cluster_id, sa_id)
    for item in api_key:
        key = item["key"]
        value = {
            "env_id": env_id,
            "env_name": env_name,
            "cluster_name": cluster_name,
            "sa_name": sa_name,
            "api_key": item,
        }
        OUTPUT_API_KEYS[key] = value


def run_api_key_workflow(
    sa_name: str,
    create_sa_account_if_necessary: bool,
    enable_key_creation: bool,
    cluster_list: list,
    force_all_clusters: bool,
    force_new_api_keys: bool,
    generate_api_keys_output_file: bool,
):
    if not (sa_name):
        raise Exception("Provide the Name for which the Service Account needs to be created")

    if enable_key_creation and not cluster_list and not force_all_clusters:
        raise Exception(
            "Trying to setup api keys but cluster ID's are not provided (--cluster-id) nor is the --force-all-clusters switch enabled."
        )

    print("=" * 40)
    clusters.run_cluster_workflow()
    print("=" * 40)
    sa_value = service_account.run_sa_workflow(sa_name, create_sa_account_if_necessary, False)
    if not create_sa_account_if_necessary and sa_value is None:
        raise Exception(
            "Could not locate Service account with Name "
            + sa_name
            + " and service account creation was not enabled. Cannot create API Keys without Service Account details."
        )
    print("=" * 40)
    int_confluent_cli_login()
    sa_id = sa_value["id"]
    sa_name = sa_value["display_name"]
    if cluster_list and not force_all_clusters:
        for item in cluster_list:
            cluster = clusters.CLUSTER_VALUES[item]
            if cluster is None:
                raise Exception("No Clusters found matching the provided Cluster ID: " + item + ".")
            int_check_setup_api_key(sa_id, sa_name, cluster, force_new_api_keys)
    if force_all_clusters:
        for cluster in clusters.CLUSTER_VALUES.values():
            int_check_setup_api_key(sa_id, sa_name, cluster, force_new_api_keys)

    if generate_api_keys_output_file:
        with open("api_key_values.json", "w", encoding="utf-8") as output_file:
            output_file.write(dumps(OUTPUT_API_KEYS))
            print("The details of api keys are added to " + output_file.name)

    return OUTPUT_API_KEYS, OUTPUT_NEWLY_CREATED_KEYS


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application",
        add_help=True,
    )
    sa_args = parser.add_argument_group("sa-args", "Service Account Arguments")
    sa_args.add_argument(
        "--service-account-name",
        type=str,
        default=None,
        help="Provide the name for which Service Account needs to be created.",
    )
    sa_args.add_argument(
        "--force-new-account",
        action="store_true",
        default=False,
        help="Force Generate a new Service Account even if an account exists with the same Name",
    )

    api_args = parser.add_argument_group("api-args", "API management arguments")
    api_args.add_argument(
        "--setup-api-keys-for-clusters",
        action="store_true",
        default=True,
        help="Generate new API Keys & Secrets while setting up the new Service Account",
    )
    api_args.add_argument(
        "--cluster-id",
        type=str,
        metavar="lkc-123456",
        default=[],
        action="append",
        dest="cluster_list",
        help="The Clusters list for which the api keys will be set up. ",
    )
    api_args.add_argument(
        "--force-all-clusters",
        action="store_true",
        default=False,
        help="If the API keys are needed for all available clusters, then --cluster-id could be ignored and this switch could be enabled.",
    )
    api_args.add_argument(
        "--force-new-api-keys",
        action="store_true",
        default=False,
        help="This will generate new API Keys even if there are existing API keys linked to a cluster already.",
    )
    api_args.add_argument(
        "--generate-api-keys-output-file",
        action="store_true",
        default=False,
        help="This will generate new API Keys even if there are existing API keys linked to a cluster already.",
    )

    args = parser.parse_args()

    base_ccloud.initial_setup(args.setup_api_keys_for_clusters)
    run_api_key_workflow(
        args.service_account_name,
        args.force_new_account,
        args.setup_api_keys_for_clusters,
        args.cluster_list,
        args.force_all_clusters,
        args.force_new_api_keys,
        args.generate_api_keys_output_file,
    )
    print("")
