import yaml
from ccloud_managers.types import CCloudConfigBundle


def create_definitions_file(def_file_path: str, ccloud_bundle: CCloudConfigBundle):
    output = {"service_accounts": []}
    for item in ccloud_bundle.cc_service_accounts.sa.values():
        acc = {
            "name": item.name,
            "description": item.description,
            "enable_rest_proxy_access": False,
            "team_email_address": "abc@abc.com",
            "api_key_access": [],
        }
        output["service_accounts"].append(acc)
    with open(def_file_path, "w") as f:
        yaml.dump(output, f, sort_keys=False)
