from requests.auth import HTTPBasicAuth

from yaml_parser import CSMConfig
from helpers import mandatory_check


class URIDetails:
    base_url = "https://api.confluent.cloud"
    environments = "/org/v2/environments"
    service_accounts = "/iam/v2/service-accounts"
    api_keys = "/iam/v2/api-keys"
    clusters = "/cmk/v2/clusters"

    def get_endpoint_url(self, key="/"):
        return self.base_url + key


class CCloudConnection:
    uri = URIDetails()

    def __init__(self, csm_configs: CSMConfig) -> None:
        print("Checking for Mandatory parameters")
        mandatory_check("api_key", csm_configs.ccloud.api_key)
        mandatory_check("api_secret", csm_configs.ccloud.api_secret)
        self.api_http_basic_auth = HTTPBasicAuth(csm_configs.ccloud.api_key, csm_configs.ccloud.api_secret)
        mandatory_check("ccloud_user", csm_configs.ccloud.ccloud_user)
        self.ccloud_user = csm_configs.ccloud.ccloud_user
        mandatory_check("ccloud_password", csm_configs.ccloud.ccloud_password)
        self.ccloud_pass = csm_configs.ccloud.ccloud_password
        print("All Good. Moving forward.")

    def get_endpoint_url(self, key="/"):
        return self.uri.base_url + key
