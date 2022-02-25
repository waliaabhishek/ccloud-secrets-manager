from abc import ABC, abstractmethod
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
        self, secret_name_prefix: str, seperator: str, env_id: str, cluster_id: str, sa_id: str
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
