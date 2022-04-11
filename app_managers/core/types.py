from dataclasses import dataclass, field
from typing import List, Literal, Tuple

from app_managers.helpers import check_pair
from app_managers.helpers import pretty as pp


class SupportedSecretStores:
    AWS_SECRETS = "aws-secretsmanager"

    def validate_store(self, secret_manager: str) -> Tuple[str, bool]:
        if secret_manager == self.AWS_SECRETS:
            return self.AWS_SECRETS, True
        else:
            return None, False

    def list_supported_stores(self):
        return [self.AWS_SECRETS]


SUPPORTED_STORES = SupportedSecretStores()


@dataclass(kw_only=True)
class CSMYAMLCCloudConfigs:
    api_key: str
    api_secret: str
    ccloud_user: str
    ccloud_password: str
    rest_proxy_secret_name: str
    ignore_service_account_list: List[str] = field(default_factory=list)
    detect_ignore_ccloud_internal_accounts: bool = False
    enable_sa_cleanup: bool = False

    def __post_init__(self) -> None:
        check_pair("api_key", self.api_key, "api_secret", self.api_secret)
        check_pair("ccloud_user", self.ccloud_user, "ccloud_password", self.ccloud_password)


@dataclass(kw_only=True)
class CSMYAMLSecretStoreConfigs:
    is_enabled: bool
    store_type: str
    configs: dict = field(default_factory=dict)
    prefix: str = field(default="")
    separator: str = field(default="/")

    def __post_init__(self) -> None:
        temp, store_enabled = SUPPORTED_STORES.validate_store(self.store_type)
        if store_enabled:
            self.store_type = temp
        else:
            raise Exception(
                type
                + " secret store manager is not supported. The supported values are "
                + ",".join(SUPPORTED_STORES.list_supported_stores())
            )
        temp_configs = {}
        for item in self.configs:
            for k, v in item.items():
                if isinstance(v, list):
                    inner_v = {}
                    for inner_item in v:
                        if isinstance(inner_item, dict):
                            inner_v.update(inner_item)
                    temp_configs[k] = inner_v
                else:
                    temp_configs[k] = v
        self.configs = temp_configs

    def __str__(self) -> None:
        pp.pprint(self)


@dataclass(kw_only=True)
class CSMYAMLConfigs:
    ccloud: CSMYAMLCCloudConfigs
    secretstore: CSMYAMLSecretStoreConfigs


@dataclass(kw_only=True)
class CSMYAMLServiceAccounts:
    name: str
    description: str
    email_address: str
    cluster_list: list
    is_rp_user: bool
    rp_access: bool = field(default=False)


@dataclass
class CSMYAMLDefinitions:
    sa: List[CSMYAMLServiceAccounts] = field(default_factory=list)

    def __str__(self) -> str:
        pp.pprint(self.sa)

    def add_service_account(self, csm_sa: CSMYAMLServiceAccounts):
        self.sa.append(csm_sa)

    def find_service_account(self, sa_name: str):
        for item in self.sa:
            if item.name == sa_name:
                return item
        return None


@dataclass(kw_only=True)
class CSMYAMLConfigBundle:
    csm_definitions: CSMYAMLDefinitions
    csm_configs: CSMYAMLConfigs
