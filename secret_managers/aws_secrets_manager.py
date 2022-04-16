import hashlib
import itertools
import pprint
from dataclasses import dataclass
from json import dumps, loads
from typing import Dict, List

import app_managers.core.types as CSMBundle
import boto3
from botocore.client import Config
from ccloud_managers.clusters import CCloudCluster
from ccloud_managers.service_account import CCloudServiceAccount
import ccloud_managers.types as CCloudBundle
from botocore.exceptions import ClientError
from ccloud_managers.api_key_manager import CCloudAPIKey
from secret_managers.types import CSMSecret, CSMSecretsManager

pp = pprint.PrettyPrinter(indent=2)


@dataclass(kw_only=True)
class AWSSecret(CSMSecret):
    def __post_init__(self) -> None:
        super().__post_init__()


class AWSSecretsList(CSMSecretsManager):
    secret: Dict[str, AWSSecret]
    client_reference = ""

    def __init__(
        self, csm_bundle: CSMBundle.CSMYAMLConfigBundle, ccloud_bundle: CCloudBundle.CCloudConfigBundle
    ) -> None:
        super().__init__(csm_bundle=csm_bundle, ccloud_bundle=ccloud_bundle)
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
        resp = self.client_reference.list_secrets(Filters=out_filter, **kwargs)
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise Exception(
                "AWS Secrets Manager List request failed. Please check the error and try again." + dumps(resp)
            )
        else:
            for item in resp["SecretList"]:
                self.add_to_cache(item["Name"], None, self.__flatten_secret_tags(item["Tags"]))
            next_token = resp.get("NextToken", False)
            if next_token:
                self.read_all_secrets(filter=filter, NextToken=next_token)

    def add_to_cache(self, secret_name: str, secret_value: Dict[str, str], secret_tags: Dict[str, str]) -> AWSSecret:
        if secret_tags.get("is_rest_proxy_user", "False") == "True":
            sync_needed = False
        elif secret_tags.get("sync_needed_for_rp", "True") == "True":
            sync_needed = True
        else:
            sync_needed = False
        self.secret[secret_name] = AWSSecret(
            secret_name=secret_name,
            secret_value=secret_value,
            env_id=secret_tags["env_id"],
            sa_id=secret_tags["sa_id"],
            sa_name=secret_tags["sa_name"],
            cluster_id=secret_tags["cluster_id"],
            rp_access=True if secret_tags["rest_proxy_access"] == "True" else False,
            api_key=secret_tags.get("api_key", ""),
            sync_needed_for_rp=sync_needed,
            api_keys_count=secret_tags.get("api_keys_count", "0--0"),
        )
        return self.secret[secret_name]

    def find_secret(self, sa_name: str, cluster_id: str = None, **kwargs) -> List[AWSSecret]:
        temp_sa = self.ccloud_bundle.cc_service_accounts.find_sa(sa_name)
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

    def get_parsed_secret_value(self, secret_name: str) -> Dict[str, str]:
        secret_value = self.get_secret(secret_name=secret_name)
        resp = loads(secret_value["SecretString"])
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
            **kwargs,
        }

    def create_or_update_secret(self, api_key: CCloudAPIKey, secret_name_postfix: str = None) -> CSMSecret:
        cluster = self.ccloud_bundle.cc_clusters.find_cluster(api_key.cluster_id)
        env = self.ccloud_bundle.cc_environments.find_environment(cluster.env_id)

        secret_name = self._create_secret_name_string(
            self.csm_bundle.csm_configs.secretstore.prefix,
            self.csm_bundle.csm_configs.secretstore.separator,
            env.env_id,
            cluster.cluster_id,
            api_key.owner_id,
            secret_name_postfix,
        )
        def_details = self.csm_bundle.csm_definitions.find_service_account(
            self.ccloud_bundle.cc_service_accounts.sa[api_key.owner_id].name
        )
        secret_tags = self.__render_secret_tags(
            env.display_name,
            env.env_id,
            cluster.cluster_name,
            api_key.cluster_id,
            self.ccloud_bundle.cc_service_accounts.sa[api_key.owner_id].name,
            api_key.owner_id,
            def_details.rp_access,
            api_key=api_key.api_key,
            sync_needed_for_rp=True if def_details.rp_access else False,
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
        return self.secret.get(secret_name)

    def __create_secret(self, secret_name: str, secret_values: dict, secret_tags: list):
        # print("Trying to create a secret with the following details:")
        # pp.pprint({"Secret Name": secret_name, "Secret Tags": secret_tags})
        resp = self.client_reference.create_secret(
            Name=secret_name,
            Description="API Key & Secret generated by the CI/CD process.",
            SecretString=dumps(secret_values),
            Tags=secret_tags,
        )
        # print("Secret Created Successfully. Secret Details as follows:")
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
        # pp.pprint(new_secret_tags)
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
        rp_secret_name: str,
        rp_sa_details: CCloudServiceAccount,
        rp_cluster_details: CCloudCluster,
        new_api_keys: List[CCloudAPIKey],
        secrets_with_rp_access: List[CSMSecret],
        is_rp_secret_new: bool,
        **kwargs,
    ):
        basic_key_string = "basic.txt"
        jaas_key_string = "restProxyUsers.jaas"
        if not is_rp_secret_new:
            rp_secret = self.get_parsed_secret_value(secret_name=rp_secret_name)
        else:
            rp_secret = dict()

        update_triggered, rp_secret, secrets_pending_tag_update = self._add_users_to_rest_proxy_secret_string(
            rp_secret_name=rp_secret_name,
            rp_secret_value=rp_secret,
            new_api_keys=new_api_keys,
            secrets_with_rp_access=secrets_with_rp_access,
            is_rp_secret_new=is_rp_secret_new,
            fe_users_key=basic_key_string,
            kafka_users_key=jaas_key_string,
        )

        if update_triggered:
            api_keys_count = self._get_rp_users_count(
                secret_value=rp_secret, fe_users_key=basic_key_string, kafka_users_key=jaas_key_string
            )
            if is_rp_secret_new:
                env_details = self.ccloud_bundle.cc_environments.find_environment(rp_cluster_details.env_id)
                secret_tags = self.__render_secret_tags(
                    env_name=env_details.display_name,
                    env_id=env_details.env_id,
                    cluster_name=rp_cluster_details.cluster_name,
                    cluster_id=rp_cluster_details.cluster_id,
                    sa_name=rp_sa_details.name,
                    sa_id=rp_sa_details.resource_id,
                    rest_proxy_access=False,
                    is_rest_proxy_user=True,
                    api_keys_count=api_keys_count["api_keys_count"],
                    # api_key=api_key.api_key,
                )
                self.__create_secret(rp_secret_name, rp_secret, self.__render_secret_tags_format(secret_tags))
                self.add_to_cache(rp_secret_name, rp_secret, secret_tags)
            else:
                response = self.client_reference.put_secret_value(
                    SecretId=rp_secret_name, SecretString=dumps(rp_secret)
                )
                self.add_tags(secret_name=rp_secret_name, tags=api_keys_count)
                print(f"Secret Successfully updated. Response\n {response}")
        for secret in itertools.chain(secrets_with_rp_access, secrets_pending_tag_update):
            self.add_tags(secret_name=secret.secret_name, tags={"sync_needed_for_rp": "False"})

    def add_tags(self, secret_name: str, tags: Dict[str, str]):
        aws_tags = self.__render_secret_tags_format(tags=tags)
        self.client_reference.tag_resource(
            SecretId=secret_name,
            Tags=aws_tags,
        )
