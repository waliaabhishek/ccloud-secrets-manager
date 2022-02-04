from typing import List, Tuple

import yaml

import helpers
from helpers import pretty as pp


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


class CSMCCloudConfig:
    def __init__(self, api_key, api_secret, ccloud_user, ccloud_pass, enable_sa_cleanup=False) -> None:
        self.__check_pair("api_key", api_key, "api_secret", api_secret)
        self.api_key = api_key
        self.api_secret = api_secret
        self.__check_pair("ccloud_user", ccloud_user, "ccloud_password", ccloud_pass)
        self.ccloud_user = ccloud_user
        self.ccloud_password = ccloud_pass
        self.enable_sa_cleanup = enable_sa_cleanup

    def __check_pair(self, key1Name, key1Value, key2Name, key2Value):
        if (key1Value and not key2Value) or (not key1Value and key2Value) or (not key1Value and not key2Value):
            raise Exception("Both " + key1Name + " & " + key2Name + " must be present in the configuration.")
        return


class CSMSecretStoreConfig:
    def __init__(self, is_enabled: bool, type: str, configs: list, prefix: str = "", separator: str = "/") -> None:
        self.enabled = is_enabled
        store_type, isEnabled = SUPPORTED_STORES.validate_store(type)
        self.prefix = prefix
        self.separator = separator
        if isEnabled:
            self.type = store_type
        else:
            raise Exception(
                type
                + " secret store manager is not supported. The supported values are "
                + ",".join(SUPPORTED_STORES.list_supported_stores())
            )
        temp = {}
        for item in configs:
            for k, v in item.items():
                temp[k] = v
        self.configs = temp

    def __str__(self) -> str:
        pp.pprint(self)


class CSMConfig:
    ccloud: CSMCCloudConfig
    secretstore: CSMSecretStoreConfig

    def __init__(self) -> None:
        pass

    def add_ccloud_configs(self, api_key, api_secret, ccloud_user, ccloud_pass, enable_sa_cleanup=False):
        self.ccloud = CSMCCloudConfig(api_key, api_secret, ccloud_user, ccloud_pass, enable_sa_cleanup)

    def add_secret_store_configs(
        self, is_enabled: bool, type: str, configs: list, prefix: str = "", separator: str = "/"
    ):
        self.secretstore = CSMSecretStoreConfig(is_enabled, type, configs, prefix, separator)


class CSMServiceAccount:
    def __init__(self, name: str, desc: str, rp_access: bool, email_adddress: str, clusters: list) -> None:
        self.name = name
        self.description = desc
        self.rp_access = rp_access
        self.email_address = email_adddress
        self.cluster_list = clusters


class CSMDefinitions:
    sa: List[CSMServiceAccount]

    def __init__(self) -> None:
        self.sa = []

    def __str__(self) -> str:
        pp.pprint(self.sa)

    def add_service_account(self, name: str, desc: str, rp_access: bool, email_adddress: str, clusters: list):
        temp = CSMServiceAccount(name, desc, rp_access, email_adddress, clusters)
        self.sa.append(temp)

    def find_service_account(self, sa_name: str):
        for item in self.sa:
            if item.name == sa_name:
                return item
        return None


def load_parse_yamls(
    config_yaml_path: str, def_yaml_path: str, generate_def_yaml: bool = False
) -> Tuple[CSMConfig, CSMDefinitions]:
    print("Trying to parse Configuration File: " + config_yaml_path)
    with open(config_yaml_path, "r") as config_file:
        csm_config = yaml.safe_load(config_file)
    helpers.env_parse_replace(csm_config)

    csm = CSMConfig()
    temp = csm_config["configs"]["ccloud_configs"]

    csm.add_ccloud_configs(
        temp["api_key"],
        temp["api_secret"],
        temp["ccloud_user"],
        temp["ccloud_password"],
        temp["enable_sa_cleanup"] if "enable_sa_cleanup" in temp else False,
    )
    temp = csm_config["configs"]["secret_store"]
    csm.add_secret_store_configs(
        temp["enabled"], temp["type"], temp["configs"], temp.get("prefix", ""), temp.get("separator", "/")
    )

    if not generate_def_yaml:
        print("Trying to parse Definitions File: " + def_yaml_path)
        with open(def_yaml_path, "r") as definitions_file:
            csm_definition = yaml.safe_load(definitions_file)
        helpers.env_parse_replace(csm_definition)
        definitions = CSMDefinitions()
        for item in csm_definition["service_accounts"]:
            definitions.add_service_account(
                item["name"],
                item["description"],
                item["enable_rest_proxy_access"],
                item["team_email_address"],
                item["api_key_access"],
            )
        return csm, definitions
    else:
        print("Not parsing Definitions file as generate flag is turned on.")
        return csm, None


if __name__ == "__main__":
    CSMData, Definitions = load_parse_yamls("config.yaml", "definitions.yaml")
    print("")
