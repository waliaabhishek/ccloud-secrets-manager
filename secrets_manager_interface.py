from abc import ABC, abstractmethod
from typing import Dict, List

from service_account import CCloudServiceAccountList


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
        # seperator = "/"
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
    def find_secret(self, sa_name: str, sa_list: CCloudServiceAccountList, cluster_id: str = None) -> List[CSMSecret]:
        pass

    @abstractmethod
    def create_or_update_secret():
        pass
