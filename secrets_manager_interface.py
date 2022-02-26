import re
from abc import ABC, abstractmethod
from tokenize import String
from typing import Dict, List, Tuple

from api_key_manager import CCloudAPIKey, CCloudAPIKeyList
from data_parser import CSMDefinitions
from service_account import CCloudServiceAccount, CCloudServiceAccountList


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
        rp_access: bool,
    ) -> None:
        self.secret_name = secret_name
        self.secret_value = secret_value
        self.env_id = env_id
        self.sa_id = sa_id
        self.sa_name = sa_name
        self.cluster_id = cluster_id
        self.api_key = api_key
        self.rp_access = rp_access


class CSMSecretsList(ABC):
    secret: Dict[str, CSMSecret]

    def __init__(self) -> None:
        pass

    @abstractmethod
    def read_all_secrets(self, filter: Dict[str, List[str]], **kwargs):
        pass

    @abstractmethod
    def find_secret(
        self, sa_name: str, sa_list: CCloudServiceAccountList, cluster_id: str = None, **kwargs
    ) -> List[CSMSecret]:
        pass

    @abstractmethod
    def create_or_update_secret(**kwargs):
        pass

    @abstractmethod
    def create_update_rest_proxy_secret(
        self, csm_definitions: CSMDefinitions, ccloud_api_key_list: CCloudAPIKeyList, **kwargs
    ):
        pass

    def __create_secret_name_string(
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
            + seperator
            + "ccloud"
            + seperator
            + sa_id
            + seperator
            + env_id
            + seperator
            + cluster_id
            + (str(seperator + secret_name_postfix) if secret_name_postfix else "")
        )
        return secret_name

    # This method will list all the newly created API Keys that have been flagged as needed REST Proxy access
    def __get_new_rest_proxy_api_keys(
        self,
        csm_definitions: CSMDefinitions,
        ccloud_api_key_list: CCloudAPIKeyList,
    ) -> List[CCloudAPIKey]:
        sa_names = [v for v in csm_definitions.sa if v.rp_access]
        api_key_details = [v for v in ccloud_api_key_list.api_keys.values() if v.owner_id in sa_names and v.api_secret]
        return api_key_details

    # This method locates the actual REST proxy Service Accounts, they are necessary
    def __get_rest_proxy_user(
        self,
        csm_definitions: CSMDefinitions,
        ccloud_api_key_list: CCloudAPIKeyList,
    ) -> List[CCloudAPIKey]:
        sa_names = [v for v in csm_definitions.sa if v.rp_access]
        api_key_details = [v for v in ccloud_api_key_list.api_keys.values() if v.owner_id in sa_names and v.api_secret]
        return api_key_details

    def __render_rp_fe_user_string(self, api_key: str, api_secret: str, postfix: str) -> String:
        return f"{api_key}: {api_secret},{postfix}"

    def __read_rp_fe_users(self, users_string: str) -> Dict[str, str]:
        output = [v for v in users_string.splitlines() if v]
        result = dict()
        for i, item in enumerate(output):
            secret, _, _ = item.partition(",krp-users")
            key, _, value = secret.partition(":")
            key, value = key.strip(), value.strip()
            result[key] = value
        return result

    def __add_front_end_user_to_rp_secret_string(
        self, secret_name: str, curr_secret_string: str, new_api_key: str, new_api_secret: str
    ) -> Tuple[bool, str]:
        basic_users = self.__read_rp_fe_users(curr_secret_string)
        no_update_required, found_update_required, update_resp = False, False, ""
        # Determine what needs to be done -- Add, Update or Skip adding the new key.
        for key, secret in basic_users.items():
            if new_api_key == key and new_api_secret == secret:
                no_update_required = True
                break
            if new_api_key == key and new_api_secret != secret:
                basic_users[key] = self.__render_rp_fe_user_string(new_api_key, new_api_secret, "krp-users")
                found_update_required = True
        # If no update is required return the same string and pass back the modified parameter as False.
        if no_update_required:
            print(f"Found the API Key in 'basic.txt' key in {secret_name} secret")
            return (False, curr_secret_string)
        elif found_update_required:
            print("API Key located but the secret value was different.")
            output_string = "\n".join(self.__render_rp_fe_user_string(k, v) for k, v in basic_users.items())
            return (True, output_string)
        else:
            print(f"API Key not found in the secret. Updating the secret now.")
            basic_users[new_api_key] = new_api_secret
            output_string = "\n".join(self.__render_rp_fe_user_string(k, v) for k, v in basic_users.items())
            return (True, output_string)

    def __read_rp_kafka_users(self, users_string: str) -> Tuple[str, str, Dict[str, str]]:
        # Partition the data before and after the KafkaClient keyword.
        # Any data before this keyword will not be edited and returned as is.
        prepend, kclient, data = users_string.partition("KafkaClient")
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

    def __render_rp_kafka_user_string(
        self, api_key: str, api_secret: str, key_type: str = "org.apache.kafka.common.security.plain.PlainLoginModule"
    ) -> String:
        return f'  {key_type} required\n  username="{api_key}"\n  password="{api_secret}";\n\n'

    def __add_kafka_user_to_rp_secret_string(
        self, secret_name: str, curr_secret_string: str, new_api_key: str, new_api_secret: str
    ) -> Tuple[bool, str]:
        prepend, postpend, jaas_users = self.__read_rp_kafka_users(curr_secret_string)
        no_update_required, found_update_required, update_resp = False, False, ""
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
            in_string = "".join(self.__render_rp_kafka_user_string(k, v) for k, v in jaas_users.items())
            output_string = "".join((prepend, in_string, postpend))
            return (True, output_string)
        else:
            print(f"API Key not found in the secret. Updating the secret now.")
            jaas_users[new_api_key] = new_api_secret
            in_string = "".join(self.__render_rp_kafka_user_string(k, v) for k, v in jaas_users.items())
            output_string = "".join((prepend, in_string, postpend))
            return (True, output_string)
