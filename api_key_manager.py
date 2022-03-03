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
