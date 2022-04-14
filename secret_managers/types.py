from dataclasses import dataclass
import re
from abc import ABC, abstractmethod
from tokenize import String
from typing import Dict, List, Tuple

from ccloud_managers.api_key_manager import CCloudAPIKey
from ccloud_managers.clusters import CCloudCluster
from ccloud_managers.service_account import CCloudServiceAccount
import ccloud_managers.types as CCloudBundle
import app_managers.core.types as CSMBundle


@dataclass(kw_only=True)
class CSMSecret:
    secret_name: str
    secret_value: Dict[str, str]
    env_id: str
    sa_id: str
    sa_name: str
    cluster_id: str
    api_key: str
    rp_access: bool
    sync_needed_for_rp: bool

    def __post_init__(self) -> None:
        pass


class CSMSecretsManager(ABC):
    csm_bundle: CSMBundle.CSMYAMLConfigBundle
    ccloud_bundle: CCloudBundle.CCloudConfigBundle
    secret: Dict[str, CSMSecret]

    def __init__(
        self, csm_bundle: CSMBundle.CSMYAMLConfigBundle, ccloud_bundle: CCloudBundle.CCloudConfigBundle
    ) -> None:
        self.csm_bundle = csm_bundle
        self.ccloud_bundle = ccloud_bundle

    @abstractmethod
    def login(self):
        pass

    @abstractmethod
    def test_login(self) -> bool:
        pass

    @abstractmethod
    def read_all_secrets(self, filter: Dict[str, List[str]], **kwargs):
        pass

    @abstractmethod
    def find_secret(self, sa_name: str, cluster_id: str = None, **kwargs) -> List[CSMSecret]:
        pass

    @abstractmethod
    def create_or_update_secret(**kwargs):
        pass

    @abstractmethod
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
        pass

    def _create_secret_name_string(
        self,
        secret_name_prefix: str,
        seperator: str,
        env_id: str,
        cluster_id: str,
        sa_id: str,
        secret_name_postfix: str = None,
        **kwargs,
    ):
        secret_name = (
            (str(seperator + secret_name_prefix) if secret_name_prefix else "")
            + f"{seperator}ccloud{seperator}{sa_id}{seperator}{env_id}{seperator}{cluster_id}"
            + (str(seperator + secret_name_postfix) if secret_name_postfix else "")
        )
        return secret_name

    # This method will list all the newly created API Keys that have been flagged as needed REST Proxy access
    def _get_new_rest_proxy_api_keys(self) -> List[CCloudAPIKey]:
        sa_names = [v for v in self.csm_bundle.csm_definitions.sa if v.rp_access]
        sa_id: List[str] = []
        for item in sa_names:
            sa_details = self.ccloud_bundle.cc_service_accounts.find_sa(sa_name=item)
            if sa_details:
                sa_id.append(sa_details.resource_id)
        api_key_details = [
            v for v in self.ccloud_bundle.cc_api_keys.api_keys.values() if v.owner_id in sa_id and v.api_secret
        ]
        return api_key_details

    # This method locates the actual REST proxy Service Accounts, they are necessary
    def _get_rest_proxy_users(self) -> List[CCloudAPIKey]:
        sa_names = [v for v in self.csm_bundle.csm_definitions.sa if v.is_rp_user]
        api_key_details = [
            v for v in self.ccloud_bundle.cc_api_keys.api_keys.values() if v.owner_id in sa_names and v.api_secret
        ]
        return api_key_details

    def _get_rest_proxy_user(self, sa_name: str, cluster_id: str) -> Tuple[str, CCloudServiceAccount, CCloudCluster]:
        cluster_details = self.ccloud_bundle.cc_clusters.find_cluster(cluster_id)
        sa_details = self.ccloud_bundle.cc_service_accounts.find_sa(sa_name=sa_name)
        rp_secret_name = self._create_secret_name_string(
            secret_name_prefix=self.csm_bundle.csm_configs.secretstore.prefix,
            seperator=self.csm_bundle.csm_configs.secretstore.separator,
            env_id=cluster_details.env_id,
            cluster_id=cluster_details.cluster_id,
            sa_id=sa_details.resource_id,
            secret_name_postfix=self.csm_bundle.csm_configs.ccloud.rest_proxy_secret_name,
        )
        return (rp_secret_name, sa_details, cluster_details)

    def _render_rp_fe_user_string(self, api_key: str, api_secret: str, postfix: str) -> String:
        return f"{api_key}: {api_secret},{postfix}"

    def _read_rp_fe_users(self, users_string: str) -> Dict[str, str]:
        output = [v for v in users_string.splitlines() if v]
        result = dict()
        for i, item in enumerate(output):
            secret, _, _ = item.partition(",krp-users")
            key, _, value = secret.partition(":")
            key, value = key.strip(), value.strip()
            result[key] = value
        return result

    def _add_front_end_user_to_rp_secret_string(
        self, secret_name: str, curr_secret_string: str, new_api_key: str, new_api_secret: str
    ) -> Tuple[bool, str]:
        basic_users = self._read_rp_fe_users(curr_secret_string)
        no_update_required, found_update_required = False, False
        # Determine what needs to be done -- Add, Update or Skip adding the new key.
        for key, secret in basic_users.items():
            if new_api_key == key and new_api_secret == secret:
                no_update_required = True
                break
            if new_api_key == key and new_api_secret != secret:
                basic_users[key] = self._render_rp_fe_user_string(new_api_key, new_api_secret, "krp-users")
                found_update_required = True
        # If no update is required return the same string and pass back the modified parameter as False.
        if no_update_required:
            print(f"Found the API Key in 'basic.txt' key in {secret_name} secret")
            return (False, curr_secret_string)
        elif found_update_required:
            print("API Key located but the secret value was different.")
            output_string = "\n".join(
                self._render_rp_fe_user_string(k, v, "krp-users") for k, v in basic_users.items()
            )
            return (True, output_string)
        else:
            print("API Key not found in the secret. Updating the secret now.")
            basic_users[new_api_key] = new_api_secret
            output_string = "\n".join(
                self._render_rp_fe_user_string(k, v, "krp-users") for k, v in basic_users.items()
            )
            return (True, output_string)

    def _read_rp_kafka_users(self, users_string: str) -> Tuple[str, str, Dict[str, str]]:
        # Partition the data before and after the KafkaClient keyword.
        # Any data before this keyword will not be edited and returned as is.
        prepend, kclient, data = users_string.partition("KafkaClient")
        if not users_string:
            prepend = """KafkaRest {
    org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
    debug="true"
    file="/mnt/secrets/5g-ndc-int-rp-users/basic.txt";
};

"""
            kclient = "KafkaClient"
        # Now split all the usernames/passwords to a list for ease of parsing.
        # The first item is the bracket right before the first pair start and hence is ignored.
        data = data.split("org.apache.kafka.common.security.plain.PlainLoginModule required")[1:]
        return_data = {}
        # Find all the usernames and password using quote based regex.
        # Anything inside quotes will be regarded as a value.
        for item in data:
            pair = re.findall(r'"(.*?)"', item)
            return_data[pair[0]] = pair[1]
            # return_data.append({"username": pair[0], "password": pair[1]})
        # Change the prepend to whatever was ignored or stripped while formatting data.
        prepend = str(prepend + kclient + " {\n")
        # The closing braces that will be required after all the username/passwords are added.
        postpend = "};\n"
        return prepend, postpend, return_data

    def _render_rp_kafka_user_string(
        self, api_key: str, api_secret: str, key_type: str = "org.apache.kafka.common.security.plain.PlainLoginModule"
    ) -> String:
        return f'  {key_type} required\n  username="{api_key}"\n  password="{api_secret}";\n\n'

    def _add_kafka_users_to_rp_secret_string(
        self, secret_name: str, curr_secret_string: str, new_api_key: str, new_api_secret: str
    ) -> Tuple[bool, str]:
        prepend, postpend, jaas_users = self._read_rp_kafka_users(curr_secret_string)
        no_update_required, found_update_required = False, False
        for key, value in jaas_users.items():
            if key == new_api_key and value == new_api_secret:
                no_update_required = True
                break
            if key == new_api_key and value != new_api_secret:
                jaas_users[key] = new_api_secret
                found_update_required = True
        if no_update_required:
            print(f"Found the API Key in 'restProxyUsers.jaas' key in {secret_name} secret")
            return (False, curr_secret_string)
        elif found_update_required:
            print("API Key located but the secret value was different.")
            in_string = "".join(self._render_rp_kafka_user_string(k, v) for k, v in jaas_users.items())
            output_string = "".join((prepend, in_string, postpend))
            return (True, output_string)
        else:
            print("API Key not found in the secret. Updating the secret now.")
            jaas_users[new_api_key] = new_api_secret
            in_string = "".join(self._render_rp_kafka_user_string(k, v) for k, v in jaas_users.items())
            output_string = "".join((prepend, in_string, postpend))
            return (True, output_string)
