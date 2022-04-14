import app_managers.core.types as types
import app_managers.helpers as helpers
import yaml


def initialize(
    config_yaml_path: str, def_yaml_path: str, generate_def_yaml: bool = False
) -> types.CSMYAMLConfigBundle:
    print("Trying to parse Configuration File: " + config_yaml_path)
    with open(config_yaml_path, "r") as config_file:
        csm_config = yaml.safe_load(config_file)
    helpers.env_parse_replace(csm_config)

    temp = csm_config["configs"]["ccloud_configs"]
    csm_ccloud_configs = types.CSMYAMLCCloudConfigs(
        api_key=temp["api_key"],
        api_secret=temp["api_secret"],
        ccloud_user=temp["ccloud_user"],
        ccloud_password=temp["ccloud_password"],
        rest_proxy_secret_name=temp.get("rest_proxy_secret_name", None),
        ignore_service_account_list=temp["ignore_service_account_list"]
        if "ignore_service_account_list" in temp
        else None,
        detect_ignore_ccloud_internal_accounts=temp["detect_ignore_ccloud_internal_accounts"]
        if "detect_ignore_ccloud_internal_accounts" in temp
        else False,
        enable_sa_cleanup=temp["enable_sa_cleanup"] if "enable_sa_cleanup" in temp else False,
        enable_api_key_cleanup=temp["enable_api_key_cleanup"] if "enable_api_key_cleanup" in temp else False,
    )

    temp = csm_config["configs"]["secret_store"]
    csm_secret_store_configs = types.CSMYAMLSecretStoreConfigs(
        is_enabled=temp["enabled"],
        store_type=temp["type"],
        configs=temp["configs"],
        prefix=temp.get("prefix", ""),
        separator=temp.get("separator", "/"),
    )

    csm_configs = types.CSMYAMLConfigs(ccloud=csm_ccloud_configs, secretstore=csm_secret_store_configs)

    if not generate_def_yaml:
        print("Trying to parse Definitions File: " + def_yaml_path)
        with open(def_yaml_path, "r") as definitions_file:
            input_definition = yaml.safe_load(definitions_file)
        helpers.env_parse_replace(input_definition)

        csm_definitions = types.CSMYAMLDefinitions()
        for item in input_definition["service_accounts"]:
            rp_user = item.get("is_rest_proxy_user", False)
            rp_access = item.get("enable_rest_proxy_access", False)
            if (rp_access or rp_user) and not csm_configs.ccloud.rest_proxy_secret_name:
                raise Exception(
                    "rest_proxy_secret_name is required in secret configuration if enable_rest_proxy_access or is_rest_proxy_user is turned on in definitions."
                )
            csm_sa = types.CSMYAMLServiceAccounts(
                name=item["name"],
                description=item["description"],
                email_address=item.get("team_email_address", None),
                cluster_list=item["api_key_access"],
                is_rp_user=rp_user,
                rp_access=True if rp_user else rp_access,
            )
            csm_definitions.add_service_account(csm_sa=csm_sa)
        return types.CSMYAMLConfigBundle(csm_configs=csm_configs, csm_definitions=csm_definitions)
    else:
        print("Not parsing Definitions file as generate flag is turned on.")
        return types.CSMYAMLConfigBundle(csm_configs=csm_configs, csm_definitions=None)


if __name__ == "__main__":
    csm_bundle = initialize("config.yaml", "definitions.yaml")
    print("")
