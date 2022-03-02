import hashlib
import pprint
from json import dumps, loads
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError

import base_ccloud
from api_key_manager import CCloudAPIKey, CCloudAPIKeyList
from clusters import CCloudClusterList, CCloudEnvironmentList
from yaml_parser import CSMConfig, CSMDefinitions
from secrets_manager_interface import CSMSecret, CSMSecretsList
from service_account import CCloudServiceAccountList

pp = pprint.PrettyPrinter(indent=2)
client = boto3.client("secretsmanager")


class AWSSecret(CSMSecret):
    def __init__(self, secret_name: str, secret_value: Dict[str, str], secret_tags: Dict[str, str]) -> None:
        super().__init__(
            secret_name,
            secret_value,
            secret_tags["env_id"],
            secret_tags["sa_id"],
            secret_tags["sa_name"],
            secret_tags["cluster_id"],
            secret_tags["api_key"],
            True if secret_tags["rest_proxy_access"].upper() == "TRUE" else False,
        )


class AWSSecretsList(CSMSecretsList):
    secret: Dict[str, AWSSecret]

    def __init__(self) -> None:
        self.secret = {}

    def __create_filter_tags(self, filter: Dict[str, List[str]]):
        output_filter = []
        for k, v in filter.items():
            output_filter.append({"Key": "tag-key", "Values": [k]})
            output_filter.append({"Key": "tag-value", "Values": v})
        return output_filter

    def __render_secret_tags_format(self, tags: dict):
        return [{"Key": str(k), "Value": str(v)} for k, v in tags.items()]

    def __flatten_secret_tags(self, tags: List[Dict[str, str]]) -> Dict[str, str]:
        return {item["Key"]: item["Value"] for item in tags}

    def __create_digest(self, json_object_data):
        output = hashlib.md5(dumps(json_object_data, sort_keys=True).encode("utf-8")).hexdigest()
        return output

    def read_all_secrets(self, filter: Dict[str, List[str]], **kwargs):
        out_filter = self.__create_filter_tags(filter)
        resp = client.list_secrets(Filters=out_filter)
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(
                "AWS Secrets Manager List request failed. Please check the error and try again." + dumps(resp)
            )
        else:
            for item in resp["SecretList"]:
                self.add_to_cache(item["Name"], None, self.__flatten_secret_tags(item["Tags"]))

    def add_to_cache(self, secret_name: str, secret_value: Dict[str, str], secret_tags: Dict[str, str]) -> AWSSecret:
        self.secret[secret_name] = AWSSecret(secret_name, secret_value, secret_tags)
        return self.secret[secret_name]

    def find_secret(
        self, sa_name: str, sa_list: CCloudServiceAccountList, cluster_id: str = None, **kwargs
    ) -> List[AWSSecret]:
        temp_sa = sa_list.find_sa(sa_name)
        if cluster_id:
            return [v for v in self.secret.values() if v.sa_id == temp_sa.resource_id and v.cluster_id == cluster_id]
        else:
            return [v for v in self.secret.values() if v.sa_id == temp_sa.resource_id]

    def get_secret(self, secret_name: str):
        try:
            resp = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                print("Secret Not Found.")
                return {}
            else:
                raise e
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(
                "AWS Secrets Manager Get Secret request failed. Please check the error and try again." + dumps(resp)
            )
        return resp

    def __render_secret_tags(
        self, env_name, env_id, cluster_name, cluster_id, sa_name, sa_id, rest_proxy_access, **kwargs
    ):
        return {
            "secret_manager": "confluent_cloud",
            "env_name": env_name,
            "env_id": env_id,
            "cluster_name": cluster_name,
            "cluster_id": cluster_id,
            "sa_name": sa_name,
            "sa_id": sa_id,
            "rest_proxy_access": rest_proxy_access,
        }

    def create_or_update_secret(
        self,
        api_key: CCloudAPIKey,
        env_list: CCloudEnvironmentList,
        cluster_list: CCloudClusterList,
        sa_list: CCloudServiceAccountList,
        csm_definitions: CSMDefinitions,
        csm_config: CSMConfig,
        secret_name_postfix: str = None,
    ) -> AWSSecret:
        cluster = cluster_list.find_cluster(api_key.cluster_id)
        env = env_list.find_environment(cluster.env_id)

        secret_name = self.__create_secret_name_string(
            csm_config.secretstore.configs.get("secret_name_prefix", ""),
            csm_config.secretstore.configs.get("secret_name_separator", "/"),
            env.env_id,
            cluster.cluster_id,
            api_key.owner_id,
            secret_name_postfix,
        )
        def_details = csm_definitions.find_service_account(sa_list.sa[api_key.owner_id].name)
        secret_tags = self.__render_secret_tags(
            env.display_name,
            env.env_id,
            cluster.cluster_name,
            api_key.cluster_id,
            sa_list.sa[api_key.owner_id].name,
            api_key.owner_id,
            def_details.rp_access,
        )
        secret_data = self.get_secret(secret_name)
        secret_value = {"username": api_key.api_key, "password": api_key.api_secret}
        if secret_data:
            self.__update_secret(
                secret_name, secret_data["SecretString"], secret_value, self.__render_secret_tags_format(secret_tags)
            )
            self.add_to_cache(secret_name, secret_value, secret_tags)
        else:
            self.__create_secret(secret_name, secret_value, self.__render_secret_tags_format(secret_tags))
            self.add_to_cache(secret_name, secret_value, secret_tags)

    def __create_secret(self, secret_name: str, secret_values: dict, secret_tags: list):
        print("Trying to create a secret with the following details:")
        pp.pprint({"Secret Name": secret_name, "Secret Tags": secret_tags})
        resp = client.create_secret(
            Name=secret_name,
            Description="API Key & Secret generated by the CI/CD process.",
            SecretString=dumps(secret_values),
            Tags=secret_tags,
        )
        print("Secret Created Successfully. Secret Details as follows:")
        return resp

    def __update_secret(
        self, secret_name: str, old_secret_values: str, new_secret_values: dict, new_secret_tags: list
    ):
        old_hash = self.__create_digest(loads(old_secret_values))
        new_hash = self.__create_digest(new_secret_values)
        if old_hash == new_hash:
            print("Not updating Secret with the provided value as current value is same as the older value.")
        else:
            print(
                "Updating "
                + secret_name
                + " with the new API Key & Secret values. API Key ID: "
                + new_secret_values["username"]
            )
            resp = client.put_secret_value(
                SecretId=secret_name,
                SecretString=dumps(new_secret_values),
            )
            print("Updated secret successfully with new API Key/Secret. Secret Details:")
            pp.pprint(resp)
        print("Adding/Updating Tags as follows:")
        pp.pprint(new_secret_tags)
        resp = client.tag_resource(SecretId=secret_name, Tags=new_secret_tags)
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print("Was not able to update the secret tags.")
        else:
            print("Tags Added successfully.")
        return

    def create_update_rest_proxy_secret(
        self,
        csm_definitions: CSMDefinitions,
        csm_configs: CSMConfig,
        ccloud_api_key_list: CCloudAPIKeyList,
        ccloud_cluster_list: CCloudClusterList,
        ccloud_sa_list: CCloudServiceAccountList,
        **kwargs
    ):
        new_api_keys = self.__get_new_rest_proxy_api_keys(csm_definitions, ccloud_api_key_list)

        cluster_list = set([v.cluster_id for v in new_api_keys])
        for item in cluster_list:
            cluster_details = ccloud_cluster_list.find_cluster(item)
            rp_secret_name = self.__create_secret_name_string(
                csm_configs.secretstore.prefix,
                csm_configs.secretstore.separator,
                cluster_details.env_id,
                cluster_details.cluster_id,
                # TODO: The Owner ID is not correct. This owner ID should come from the REST Proxy user SA; and not the API Key SA.
                api_key.owner_id,
                "rp-users",
            )
            rp_secret = self.get_secret(rp_secret_name)
            if rp_secret:
                rp_secret_data = loads(rp_secret["SecretString"])
            else:
                rp_secret_data = ""
            update_triggered = False
            for api_key in [v for v in new_api_keys if v.cluster_id == item]:
                # Rest proxy Front End String Update
                is_updated, rp_secret_data["basic.txt"] = self.__add_front_end_user_to_rp_secret_string(
                    rp_secret_name, rp_secret_data["basic.txt"], api_key.api_key, api_key.api_secret
                )
                if is_updated:
                    update_triggered = True
                is_updated, rp_secret_data["restProxyUsers.jaas"] = self.__add_kafka_user_to_rp_secret_string(
                    rp_secret_name, rp_secret_data["restProxyUsers.jaas"], api_key.api_key, api_key.api_secret
                )
                if is_updated:
                    update_triggered = True
            if update_triggered:
                # TODO: The secret data is updated and now needs to be persisted into AWS Secrets Manager.
                secret_tags = self.__render_secret_tags()
                pass


def add_secrets_to_rest_proxy_user(
    env_id: str,
    cluster_id: str,
    secret_name_prefix: str,
    api_key: str,
    api_secret: str,
    rp_fe_user_filepath: str,
    secret_tags: dict,
    rest_proxy_user="rest-proxy-user",
    proxy_user_string="krp-users",
):
    rest_proxy_secret_name = create_secret_name(env_id, cluster_id, rest_proxy_user, api_key, secret_name_prefix)
    secret_data = get_secret(rest_proxy_secret_name)
    secret_tags.pop("sa_name", None)
    secret_tags.pop("sa_id", None)
    secret_tags.pop("allowed_in_rest_proxy", None)
    secret_tags["rest_proxy_user"] = "True"
    if not secret_data:
        print("Did not find any Rest Proxy Secret for the environment and cluster. Creating a new Secret now.")
        basic_user = api_key + ": " + api_secret + "," + proxy_user_string
        jaas_user = "\n".join(
            [
                "KafkaRest {",
                "\torg.eclipse.jetty.jaas.spi.PropertyFileLoginModule required",
                '\tdebug="true"',
                str('\tfile="' + rp_fe_user_filepath + '";'),
                "};",
                "" "KafkaClient {",
                "\torg.apache.kafka.common.security.plain.PlainLoginModule required",
                str('\tusername="' + api_key + '"'),
                str('\tpassword="' + api_secret + '";'),
                "};",
            ]
        )
        secret_data = {
            "basic.txt": basic_user,
            "restProxyUsers.jaas": jaas_user,
        }
        create_secret(rest_proxy_secret_name, secret_data, render_secret_tags_format(secret_tags))
    else:
        secret_string = loads(secret_data["SecretString"])
        secret_string["basic.txt"] = "\n".join(
            [secret_string["basic.txt"], str(api_key + ": " + api_secret + "," + proxy_user_string)]
        )
        location = str(secret_string["restProxyUsers.jaas"]).rfind("};")
        if location == -1:
            raise Exception("Could not parse/understand JAAS configs. Not sure what to do.")
        secret_string["restProxyUsers.jaas"] = "\n".join(
            [
                secret_string["restProxyUsers.jaas"][:location],
                "",
                "\torg.apache.kafka.common.security.plain.PlainLoginModule required",
                str('\tusername="' + api_key + '"'),
                str('\tpassword="' + api_secret + '";'),
                secret_string["restProxyUsers.jaas"][location:],
            ]
        )
        update_secret(rest_proxy_secret_name, loads(secret_data["SecretString"]), secret_string, secret_tags)
    return


def run_aws_sec_mgr_workflow(
    wf_type: str, api_keys: dict, add_as_rest_proxy_user: bool, secret_name_prefix: str, rp_fe_user_filepath: str
):
    for api_key in api_keys.values():
        env_name = api_key["env_name"]
        env_id = api_key["env_id"]
        cluster_name = api_key["cluster_name"]
        cluster_id = api_key["resource_id"]
        sa_name = api_key["sa_name"]
        sa_id = api_key["owner_resource_id"]
        api_key_name = api_key["key"]
        rest_proxy_user = "True" if add_as_rest_proxy_user else "False"
        tags_for_create = {
            "secret_type": "confluent_cloud",
            "env_name": env_name,
            "env_id": env_id,
            "cluster_name": cluster_name,
            "cluster_id": cluster_id,
            "sa_name": sa_name,
            "sa_id": sa_id,
            "allowed_in_rest_proxy": rest_proxy_user,
        }
        secret_name = create_secret_name(env_id, cluster_id, sa_id, api_key_name, secret_name_prefix)
        # client.restore_secret(SecretId=secret_name)
        out_secret = get_secret(secret_name)
        if "get" in wf_type:
            if out_secret:
                print("Secret Found.")
                out = out_secret.copy()
                out.pop("SecretString")
                pp.pprint(out)
                return out
            else:
                print("Could not find Secret with Name " + secret_name + " and workflow was retrieve only.")
                return out_secret
        if "store" in wf_type:
            secret_data = {"username": api_key["key"], "password": api_key["secret"]}
            if not out_secret:
                create_secret(secret_name, secret_data, render_secret_tags_format(tags_for_create))
            else:
                update_secret(
                    secret_name, out_secret["SecretString"], secret_data, render_secret_tags_format(tags_for_create)
                )
            if add_as_rest_proxy_user:
                add_secrets_to_rest_proxy_user(
                    env_id,
                    cluster_id,
                    secret_name_prefix,
                    api_key["key"],
                    api_key["secret"],
                    rp_fe_user_filepath,
                    tags_for_create,
                    rest_proxy_user,
                    proxy_user_string,
                )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application",
        add_help=True,
    )

    wf_args = parser.add_argument_group("workflow-args", "Workflow Selection Arguments")
    wf_args.add_argument(
        "--wf-name",
        type=str,
        default=[],
        action="append",
        dest="wf_name",
        choices=["all", "create-sa", "get-sa", "create-apikey"],
        help="Workflow that needs to be executed",
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
        help="Force Generate a new Service Account even if an account exists with the same Name.",
    )
    sa_args.add_argument(
        "--add-as-rest-proxy-user",
        action="store_true",
        default=False,
        help="Add the Service Account to REST Proxy users",
    )

    api_args = parser.add_argument_group("api-args", "API management arguments")
    api_args.add_argument(
        "--setup-api-keys-for-clusters",
        action="store_true",
        default=False,
        help="Generate new API Keys & Secrets while setting up the new Service Account",
    )
    api_args.add_argument(
        "--create-service-account-if-necessary",
        action="store_true",
        default=False,
        help="If enabled, will create a new Service Account when trying to create a new API Key. "
        + "Otherwise, will throw an exception if Service Account is not found prior to creating the API Key.",
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

    sm_args = parser.add_argument_group("sm-args", "Secret Manager arguments")
    sm_args.add_argument(
        "--enable-secret-management",
        action="store_true",
        default=False,
        help="This switch enables the Secret Management for API Keys and Service Accounts.",
    )
    sm_args.add_argument(
        "--secret-manager-type",
        type=str,
        default="aws-secretmanager",
        choices=["aws-secretmanager"],
        help="Provide the Secret Storage Type to use",
    )
    api_args.add_argument(
        "--secret-name-prefix",
        type=str,
        metavar="prefix_for_secret_name",
        default=None,
        help="If you need to add a prefix to the Secret name, it could be done with this switch.",
    )

    args = parser.parse_args()

    base_ccloud.initial_setup(args.setup_api_keys_for_clusters)
    api_keys = {}
    with open("api_key_values.json", "r", encoding="utf-8") as input_file:
        api_keys = loads(input_file.read())
    run_aws_sec_mgr_workflow("store", api_keys, args.add_as_rest_proxy_user, args.secret_name_prefix)
    print("")
