import pprint
from json import loads, dumps
import subprocess
from operator import itemgetter

import base_ccloud
import clusters
import service_account

pp = pprint.PrettyPrinter(indent=2)
CMD_PIPE_SEP = " | "
CMD_OUT_SUPPRESS = " 1>/dev/null "
CMD_STDERR_TO_STDOUT = " 2>&1 "
SA_API_KEYS = {}
OUTPUT_API_KEYS = {}
OUTPUT_NEWLY_CREATED_KEYS = {}


def int_execute_subcommand(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out = process.communicate()[0].strip()
    return out.decode('UTF-8')


def int_confluent_cli_login():
    cmd_login = "confluent login" + CMD_STDERR_TO_STDOUT
    output = int_execute_subcommand(cmd_login)
    if output != "":
        raise Exception(
            "Could not login into Confluent Cloud CLI. Please ensure that the credentials are correct."
            + output)


def int_confluent_cli_set_env(env_id):
    cmd_set_env = "confluent environment use " + env_id
    output = int_execute_subcommand(cmd_set_env)
    if not output.startswith('Now using "' + env_id + '" as the default (active) environment.'):
        raise Exception("Could not set the current environment to " + env_id +
                        ". Please fix all the issues before trying again. " + output)


def int_confluent_cli_set_cluster(cluster_id):
    cmd_set_cluster = "confluent kafka cluster use " + \
        cluster_id + CMD_STDERR_TO_STDOUT
    output = int_execute_subcommand(cmd_set_cluster)
    if not output.startswith('Set Kafka cluster "' + cluster_id + '"'):
        raise Exception("Could not set the current cluster to " + cluster_id +
                        ". Please fix al the issues before trying again. " + output)


def int_confluent_cli_get_api_key_list(sa_id):
    cmd_api_key_list = "confluent api-key list -o json --service-account " + sa_id
    output = loads(int_execute_subcommand(cmd_api_key_list))
    output = sorted(output, key=itemgetter('created'), reverse=True)
    global SA_API_KEYS
    SA_API_KEYS = output


def int_check_existing_api_key(cluster_id, sa_id, api_key_id=None):
    temp_key = []
    if SA_API_KEYS:
        if api_key_id:
            temp_key = [item for item in SA_API_KEYS if sa_id in item["owner_resource_id"]
                        and cluster_id in item["resource_id"] and api_key_id in item["key"]]
        else:
            temp_key = sorted([item for item in SA_API_KEYS if sa_id in item["owner_resource_id"]
                               and cluster_id in item["resource_id"]], key=itemgetter('created'), reverse=True)
    return temp_key


def int_print_api_key(sa_name, api_key):
    print("Found " + str(len(api_key)) + " existing API key(s) with ID " +
          ", ".join([item["key"] for item in api_key]) + " for Service Account " + sa_name)
    pp.pprint(api_key)


def int_create_api_key(cluster_id, sa_id, sa_name):
    api_key_description = "API Key for " + sa_name + " created by CI/CD framework."
    cmd_create_api_key = "confluent api-key create -o json --service-account " + sa_id + \
        " --resource " + cluster_id + " --description \"" + \
        api_key_description + "\"" + CMD_STDERR_TO_STDOUT
    output = loads(int_execute_subcommand(cmd_create_api_key))
    return output


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
        print(
            "Override provided via --force-new-api-keys switch. Generating new API Keys.")
        api_key = None
    if not api_key:
        create_output = int_create_api_key(cluster_id, sa_id, sa_name)
        int_confluent_cli_get_api_key_list(sa_id)
        key_id = create_output["key"]
        api_key = int_check_existing_api_key(
            cluster_id, sa_id, key_id)
        int_print_api_key(sa_name, api_key)
        OUTPUT_NEWLY_CREATED_KEYS[key_id] = api_key[0].copy()
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["secret"] = create_output["secret"]
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["env_id"] = env_id
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["env_name"] = env_name
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["cluster_name"] = cluster_name
        OUTPUT_NEWLY_CREATED_KEYS[key_id]["sa_name"] = sa_name
    global OUTPUT_API_KEYS
    for item in api_key:
        key = item["key"]
        value = {
            "env_id": env_id,
            "env_name": env_name,
            "cluster_name": cluster_name,
            "sa_name": sa_name,
            "api_key": item
        }
        OUTPUT_API_KEYS[key] = value


def run_api_key_workflow(sa_name: str, create_sa_account_if_necessary: bool, enable_key_creation: bool, cluster_list: list,
                         force_all_clusters: bool, force_new_api_keys: bool, generate_api_keys_output_file: bool):
    if not (sa_name):
        raise Exception(
            'Provide the Name for which the Service Account needs to be created')

    if enable_key_creation and not cluster_list and not force_all_clusters:
        raise Exception(
            "Trying to setup api keys but cluster ID's are not provided (--cluster-id) nor is the --force-all-clusters switch enabled.")

    print("=" * 40)
    clusters.run_cluster_workflow()
    print("=" * 40)
    sa_value = service_account.run_sa_workflow(
        sa_name, create_sa_account_if_necessary, False)
    if not create_sa_account_if_necessary and sa_value is None:
        raise Exception("Could not locate Service account with Name " + sa_name +
                        " and service account creation was not enabled. Cannot create API Keys without Service Account details.")
    print("=" * 40)
    int_confluent_cli_login()
    sa_id = sa_value["id"]
    sa_name = sa_value["display_name"]
    if cluster_list and not force_all_clusters:
        for item in cluster_list:
            cluster = clusters.CLUSTER_VALUES[item]
            if cluster is None:
                raise Exception(
                    "No Clusters found matching the provided Cluster ID: " + item + ".")
            int_check_setup_api_key(
                sa_id, sa_name, cluster, force_new_api_keys)
    if force_all_clusters:
        for cluster in clusters.CLUSTER_VALUES.values():
            int_check_setup_api_key(
                sa_id, sa_name, cluster, force_new_api_keys)

    if generate_api_keys_output_file:
        with open('api_key_values.json', 'w', encoding="utf-8") as output_file:
            output_file.write(dumps(OUTPUT_API_KEYS))
            print("The details of api keys are added to " + output_file.name)

    return OUTPUT_API_KEYS, OUTPUT_NEWLY_CREATED_KEYS


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application", add_help=True, )
    sa_args = parser.add_argument_group("sa-args", "Service Account Arguments")
    sa_args.add_argument('--service-account-name', type=str, default=None,
                         help="Provide the name for which Service Account needs to be created.",)
    sa_args.add_argument('--force-new-account', action="store_true", default=False,
                         help="Force Generate a new Service Account even if an account exists with the same Name",)

    api_args = parser.add_argument_group(
        "api-args", "API management arguments")
    api_args.add_argument('--setup-api-keys-for-clusters', action="store_true", default=True,
                          help="Generate new API Keys & Secrets while setting up the new Service Account",)
    api_args.add_argument('--cluster-id', type=str, metavar="lkc-123456", default=[], action="append", dest="cluster_list",
                          help='The Clusters list for which the api keys will be set up. ')
    api_args.add_argument('--force-all-clusters', action="store_true", default=False,
                          help="If the API keys are needed for all available clusters, then --cluster-id could be ignored and this switch could be enabled.",)
    api_args.add_argument('--force-new-api-keys', action="store_true", default=False,
                          help="This will generate new API Keys even if there are existing API keys linked to a cluster already.",)
    api_args.add_argument('--generate-api-keys-output-file', action="store_true", default=False,
                          help="This will generate new API Keys even if there are existing API keys linked to a cluster already.",)

    args = parser.parse_args()

    base_ccloud.initial_setup(args.setup_api_keys_for_clusters)
    run_api_key_workflow(args.service_account_name, args.force_new_account, args.setup_api_keys_for_clusters, args.cluster_list,
                         args.force_all_clusters, args.force_new_api_keys, args.generate_api_keys_output_file)
    print("")
