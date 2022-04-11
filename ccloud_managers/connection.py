from dataclasses import dataclass, field

from app_managers.core.types import CSMYAMLConfigBundle
from app_managers.helpers import mandatory_check
from requests.auth import HTTPBasicAuth


class URIDetails:
    base_url = "https://api.confluent.cloud"
    environments = "/org/v2/environments"
    service_accounts = "/iam/v2/service-accounts"
    api_keys = "/iam/v2/api-keys"
    clusters = "/cmk/v2/clusters"


@dataclass(
    frozen=True,
    kw_only=True,
)
class CCloudConnection:
    csm_bundle: CSMYAMLConfigBundle
    uri: URIDetails = field(default_factory=URIDetails)
    http_connection: HTTPBasicAuth = field(init=False)

    def __post_init__(self) -> None:
        mandatory_check("api_key", self.csm_bundle.csm_configs.ccloud.api_key)
        mandatory_check("api_secret", self.csm_bundle.csm_configs.ccloud.api_secret)
        mandatory_check("ccloud_user", self.csm_bundle.csm_configs.ccloud.ccloud_user)
        mandatory_check("ccloud_password", self.csm_bundle.csm_configs.ccloud.ccloud_password)
        object.__setattr__(
            self,
            "http_connection",
            HTTPBasicAuth(self.csm_bundle.csm_configs.ccloud.api_key, self.csm_bundle.csm_configs.ccloud.api_secret),
        )

    def get_endpoint_url(self, key="/"):
        return self.uri.base_url + key


@dataclass
class CCloudBase:
    _ccloud_connection: CCloudConnection
    url: str = field(init=False)
    http_connection: HTTPBasicAuth = field(init=False)

    def __post_init__(self) -> None:
        self.http_connection = self._ccloud_connection.http_connection
