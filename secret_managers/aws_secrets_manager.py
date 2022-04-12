import hashlib
import pprint
from dataclasses import dataclass
from json import dumps, loads
from typing import Dict, List

import app_managers.core.types as CSMBundle
import boto3
from botocore.client import Config
import ccloud_managers.types as CCloudBundle
from botocore.exceptions import ClientError
from ccloud_managers.api_key_manager import CCloudAPIKey
from app_managers.helpers import printline
from secret_managers.types import CSMSecret, CSMSecretsManager

pp = pprint.PrettyPrinter(indent=2)


@dataclass()
class AWSSecret(CSMSecret):
    pass


class AWSSecretsList(CSMSecretsManager):
    secret: Dict[str, AWSSecret]
    client_reference = ""

    def __init__(self, csm_bundle: CSMBundle.CSMYAMLConfigBundle) -> None:
        self.csm_bundle = csm_bundle
        self.secret = {}
        self.login()
        self.read_all_secrets()

    def login(self):
        # AWS makes it pretty simple and all it needs is a few ENV variables.
        # Details here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#guide-configuration
        login_kwargs = self.csm_bundle.csm_configs.secretstore.configs
        login_kwargs["service_name"] = "secretsmanager"
        # The Additional Configs are not entertained by boto3 as kwargs.
        # We need to create a separate botocore Config object and then pass as
        extra_configs = login_kwargs.pop("config", None)
        if extra_configs:
            extra_configs = Config(**extra_configs)
        self.client_reference = boto3.client(config=extra_configs, **login_kwargs)
        if not self.test_login():
            raise Exception("Cannot set up a connection with AWS Secrets Manager. Will not be able to proceed.")

    def test_login(self) -> bool:
        # TODO: Not sure how to validate if the client is setup or not. But keeping it here, in case this needs to be replaced in the future.
        return True

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

    def read_all_secrets(
        self,
        filter: Dict[str, List[str]] = {"secret_manager": ["confluent_cloud"]},
        **kwargs,
    ):
        out_filter = self.__create_filter_tags(filter)
        resp = self.client_reference.list_secrets(Filters=out_filter)
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
        self, ccloud_bundle: CCloudBundle.CCloudConfigBundle, sa_name: str, cluster_id: str = None, **kwargs
    ) -> List[AWSSecret]:
        temp_sa = ccloud_bundle.cc_service_accounts.find_sa(sa_name)
        if cluster_id:
            return [v for v in self.secret.values() if v.sa_id == temp_sa.resource_id and v.cluster_id == cluster_id]
        else:
            return [v for v in self.secret.values() if v.sa_id == temp_sa.resource_id]

    def get_secret(self, secret_name: str):
        try:
            resp = self.client_reference.get_secret_value(SecretId=secret_name)
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
        ccloud_bundle: CCloudBundle.CCloudConfigBundle,
        csm_bundle: CSMBundle.CSMYAMLConfigBundle,
        secret_name_postfix: str = None,
    ) -> AWSSecret:
        cluster = ccloud_bundle.cc_clusters.find_cluster(api_key.cluster_id)
        env = ccloud_bundle.cc_environments.find_environment(cluster.env_id)

        secret_name = self.__create_secret_name_string(
            csm_bundle.csm_configs.secretstore.configs.get("secret_name_prefix", ""),
            csm_bundle.csm_configs.secretstore.configs.get("secret_name_separator", "/"),
            env.env_id,
            cluster.cluster_id,
            api_key.owner_id,
            secret_name_postfix,
        )
        def_details = csm_bundle.csm_definitions.find_service_account(
            ccloud_bundle.cc_service_accounts.sa[api_key.owner_id].name
        )
        secret_tags = self.__render_secret_tags(
            env.display_name,
            env.env_id,
            cluster.cluster_name,
            api_key.cluster_id,
            ccloud_bundle.cc_service_accounts.sa[api_key.owner_id].name,
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
        resp = self.client_reference.create_secret(
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
                f'Updating {secret_name} with the new API Key & Secret values. API Key ID: {new_secret_values["username"]}'
            )
            resp = self.client_reference.put_secret_value(
                SecretId=secret_name,
                SecretString=dumps(new_secret_values),
            )
            print("Updated secret successfully with new API Key/Secret. Secret Details:")
            pp.pprint(resp)
        print("Adding/Updating Tags as follows:")
        pp.pprint(new_secret_tags)
        resp = self.client_reference.tag_resource(SecretId=secret_name, Tags=new_secret_tags)
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            print("Was not able to update the secret tags.")
        else:
            print("Tags Added successfully.")
        return

    # The function could be used to update the REST Proxy Users secret for adding all the api-keys to AWS Secret.
    # It could also take a single api_key or multiple API_keys in a list as input for forcing only those API Keys to the AWS Secret.
    # Passing a single API KEY per call is not a great idea as PUT_SECRET_VALUE method creates a new version everytime in the AWS secret
    # and will cause AWS limit issues if you have too many API Keys and need them as part of REST Proxy users.
    def create_update_rest_proxy_secrets(
        self,
        ccloud_bundle: CCloudBundle.CCloudConfigBundle,
        csm_bundle: CSMBundle.CSMYAMLConfigBundle,
        new_api_keys: List[CCloudAPIKey] = None,
        **kwargs,
    ):
        printline()
        print("Triggering REST Proxy Secret Update")
        if not new_api_keys:
            new_api_keys = self.__get_new_rest_proxy_api_keys(csm_bundle.csm_definitions, ccloud_bundle.cc_api_keys)

        rest_proxy_users_list = self.__get_rest_proxy_users(csm_bundle.csm_definitions, ccloud_bundle.cc_api_keys)

        if not rest_proxy_users_list:
            raise Exception(
                "No REST Proxy users were found but the config is required to configure REST Proxy secret. Cannot proceed."
            )

        cluster_list = set([v.cluster_id for v in new_api_keys])
        for item in cluster_list:
            cluster_details = ccloud_bundle.cc_clusters.find_cluster(item)
            rest_proxy_user = ""
            for rp_iter in rest_proxy_users_list:
                if rp_iter.cluster_id == item:
                    rest_proxy_user = rp_iter
                    break
            if not rest_proxy_user:
                raise Exception(f"Could not find the REST Proxy Service Account to set up secret in {item} cluster. ")
            rp_secret_name = self.__create_secret_name_string(
                csm_bundle.csm_configs.secretstore.prefix,
                csm_bundle.csm_configs.secretstore.separator,
                cluster_details.env_id,
                cluster_details.cluster_id,
                rest_proxy_user.owner_id,
                csm_bundle.csm_configs.ccloud.rest_proxy_secret_name,
            )
            rp_secret = self.get_secret(rp_secret_name)
            if rp_secret:
                rp_secret_data = loads(rp_secret["SecretString"])
                is_new_secret = False
            else:
                rp_secret_data = ""
                is_new_secret = True
            update_triggered = False
            for api_key in [v for v in new_api_keys if v.cluster_id == item]:
                # Rest proxy Front End String Update
                is_updated, rp_secret_data["basic.txt"] = self.__add_front_end_user_to_rp_secret_string(
                    rp_secret_name, rp_secret_data.get("basic.txt", ""), api_key.api_key, api_key.api_secret
                )
                if is_updated:
                    update_triggered = True
                is_updated, rp_secret_data["restProxyUsers.jaas"] = self.__add_kafka_user_to_rp_secret_string(
                    rp_secret_name, rp_secret_data.get("restProxyUsers.jaas", ""), api_key.api_key, api_key.api_secret
                )
                if is_updated:
                    update_triggered = True
            if update_triggered:
                if is_new_secret:
                    env_details = ccloud_bundle.cc_environments.find_environment(cluster_details.env_id)
                    secret_tags = self.__render_secret_tags(
                        env_details.display_name,
                        env_details.env_id,
                        cluster_details.cluster_name,
                        cluster_details.cluster_id,
                        ccloud_bundle.cc_service_accounts.sa[rest_proxy_user.owner_id].name,
                        rest_proxy_user.owner_id,
                        True,
                    )
                    self.__create_secret(rp_secret_name, rp_secret_data, self.__render_secret_tags_format(secret_tags))
                    self.add_to_cache(rp_secret_name, rp_secret_data, secret_tags)
                else:
                    response = self.client_reference.put_secret_value(
                        SecretId=rp_secret_name, SecretString=dumps(rp_secret_data)
                    )
                    print(f"Secret Successfully updated. Response\n {response}")
