from abc import ABC, abstractmethod
import hashlib
from json import dumps, loads
from typing import Dict, List

import boto3
from botocore.exceptions import ClientError

import base_ccloud
from api_key_manager import CCloudAPIKey, CCloudAPIKeyList
from clusters import CCloudClusterList, CCloudEnvironmentList
from service_account import CCloudServiceAccountList

client = boto3.client("secretsmanager")


class CSMSecret:
    def __init__(
        self,
        secret_name: str,
        secret_value: Dict[str, str],
        env_id: str,
        sa_id: str,
        sa_name: str,
        cluster_id: str,
        api_key: str,
    ) -> None:
        self.secret_name = secret_name
        self.secret_value = secret_value
        self.env_id = env_id
        self.sa_id = sa_id
        self.sa_name = sa_name
        self.cluster_id = cluster_id
        self.api_key = api_key


class CSMSecretsList(ABC):
    secret: Dict[str, CSMSecret]

    def __init__(self) -> None:
        pass

    def create_secret_name_string(
        self, secret_name_prefix: str, seperator: str, env_id: str, cluster_id: str, sa_id: str
    ):
        seperator = "/"
        # secretName = env_name + sep + cluster_name + sep + sa_name
        secret_name = (
            (str(seperator + secret_name_prefix) if secret_name_prefix else "")
            + seperator
            + "ccloud"
            + seperator
            + sa_id
            + seperator
            + env_id
            + seperator
            + cluster_id
        )
        return secret_name

    @abstractmethod
    def read_all_secrets(self, filter: Dict[str, List[str]]):
        pass

    @abstractmethod
    def find_secret(
        self, sa_name: str, sa_list: CCloudServiceAccountList, api_key_list: CCloudAPIKeyList
    ) -> List[CSMSecret]:
        pass

    @abstractmethod
    def create_or_update_secret():
        pass


class AWSSecret(CSMSecret):
    def __init__(self, secret_name: str, secret_value: Dict[str, str], secret_tags: Dict[str, str]) -> None:
        super.__init__(
            secret_name,
            secret_value,
            secret_tags["env_id"],
            secret_tags["sa_id"],
            secret_tags["sa_name"],
            secret_tags["cluster_id"],
            secret_tags["api_key"],
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
        # output_filter = [{"Key": "tag-key", "Values": k},{"Key": "tag-value", "Values": v} for k, v in filter.items()]
        return output_filter

    def __render_secret_tags_format(self, tags: dict):
        return [{"Key": k, "Value": v} for k, v in tags.items()]

    def __create_digest(self, json_object_data):
        output = hashlib.md5(dumps(json_object_data, sort_keys=True).encode("utf-8")).hexdigest()
        return output

    def read_all_secrets(self, filter: Dict[str, List[str]]):
        out_filter = self.__create_filter_tags(filter)
        resp = client.list_secrets(Filters=out_filter)
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(
                "AWS Secrets Manager List request failed. Please check the error and try again." + dumps(resp)
            )
        else:
            # TODO: Implement caching from the response.
            # self.add_to_cache()
            pass

    def add_to_cache(self, secret_name: str, secret_value: Dict[str, str], secret_tags: Dict[str, str]) -> AWSSecret:
        self.sa[secret_name] = AWSSecret(secret_name, secret_value, secret_tags)
        return self.sa[secret_name]

    def find_secret(
        self, sa_name: str, sa_list: CCloudServiceAccountList, api_key_list: CCloudAPIKeyList
    ) -> List[AWSSecret]:
        temp_sa = sa_list.find_sa(sa_name)
        # temp_api_key_list = api_key_list.find_keys_with_sa(sa_name)
        sa_id = temp_sa.resource_id
        for item in api_key_list.find_keys_with_sa(sa_name):
            pass

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

    def create_or_update_secret(
        self,
        api_key: CCloudAPIKey,
        env_list: CCloudEnvironmentList,
        cluster_list: CCloudClusterList,
        sa_list: CCloudServiceAccountList,
    ) -> AWSSecret:
        cluster = cluster_list.find_cluster(api_key.cluster_id)
        env = env_list.find_environment(cluster.env_id)
        secret_name = self.create_secret_name_string(None, "/", env.env_id, cluster.cluster_id, api_key.owner_id)
        secret_tags = {
            "secret_manager": "confluent_cloud",
            "env_name": env.display_name,
            "env_id": env.env_id,
            "cluster_name": cluster.cluster_name,
            "cluster_id": api_key.cluster_id,
            "sa_name": sa_list.sa[api_key.owner_id].name,
            "sa_id": api_key.owner_id,
            # TODO: Enable REST Proxy access implementation is still needed
            "allowed_in_rest_proxy": False,
        }
        secret_data = self.get_secret(secret_name)
        secret_value = {"username": api_key.api_key, "password": api_key.api_secret}
        if secret_data:
            self.__update_secret(
                secret_name, secret_data["SecretString"], secret_value, self.__render_secret_tags_format(secret_tags)
            )
        else:
            self.__create_secret(secret_name, secret_value, self.__render_secret_tags_format(secret_tags))

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


# def create_secret_name(env_id: str, cluster_id: str, sa_id: str, api_key_name: str, secret_name_prefix: str):
#     sep = "/"
#     # secretName = env_name + sep + cluster_name + sep + sa_name
#     secret_name = (
#         (str(sep + secret_name_prefix) if secret_name_prefix else "")
#         + sep
#         + "ccloud"
#         + sep
#         + sa_id
#         + sep
#         + env_id
#         + sep
#         + cluster_id
#     )
#     return secret_name


# def create_filter_tags(key_value: str, tags: dict):
#     filter = [{"Key": key_value, "Values": [v]} for k, v in tags.items()]
#     return filter


# def render_secret_tags_format(tags: dict):
#     return [{"Key": k, "Value": v} for k, v in tags.items()]


# def create_digest(json_object_data):
#     output = hashlib.md5(dumps(json_object_data, sort_keys=True).encode("utf-8")).hexdigest()
#     return output


# def list_secrets(key_value: str, filter_tags: dict):
#     filter = create_filter_tags(key_value, filter_tags)
#     resp = client.list_secrets(Filters=filter)
#     if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
#         raise Exception("AWS Secrets Manager List request failed. Please check the error and try again." + dumps(resp))
#     return resp["SecretList"]


# def get_secret(secret_name: str):
#     try:
#         resp = client.get_secret_value(SecretId=secret_name)
#     except ClientError as e:
#         if e.response["Error"]["Code"] == "ResourceNotFoundException":
#             print("Secret Not Found.")
#             return {}
#         else:
#             raise e
#     if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
#         raise Exception(
#             "AWS Secrets Manager Get Secret request failed. Please check the error and try again." + dumps(resp)
#         )
#     return resp


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


# def create_secret(secret_name: str, secret_values: dict, secret_tags: list):
#     print("Trying to create a secret with the following details:")
#     pp.pprint({"Secret Name": secret_name, "Secret Tags": secret_tags})
#     resp = client.create_secret(
#         Name=secret_name,
#         Description="API Key & Secret generated by the CI/CD process.",
#         SecretString=dumps(secret_values),
#         Tags=secret_tags,
#     )
#     print("Secret Created Successfully. Secret Details as follows:")
#     return resp


# def update_secret(secret_name: str, old_secret_values: str, new_secret_values: dict, new_secret_tags: list):
#     old_hash = create_digest(loads(old_secret_values))
#     new_hash = create_digest(new_secret_values)
#     if old_hash == new_hash:
#         print("Not updating Secret with the provided value as current value is same as the older value.")
#     else:
#         print(
#             "Updating "
#             + secret_name
#             + " with the new API Key & Secret values. API Key ID: "
#             + new_secret_values["username"]
#         )
#         resp = client.put_secret_value(
#             SecretId=secret_name,
#             SecretString=dumps(new_secret_values),
#         )
#         print("Updated secret successfully with new API Key/Secret. Secret Details:")
#         pp.pprint(resp)
#     print("Adding/Updating Tags as follows:")
#     pp.pprint(new_secret_tags)
#     resp = client.tag_resource(SecretId=secret_name, Tags=new_secret_tags)
#     if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
#         print("Was not able to update the secret tags.")
#     else:
#         print("Tags Added successfully.")
#     return


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
    # run_aws_sec_mgr_workflow("get", api_keys, args.add_as_rest_proxy_user,
    #                          False, args.secret_name_prefix)
    run_aws_sec_mgr_workflow("store", api_keys, args.add_as_rest_proxy_user, args.secret_name_prefix)
    print("")
