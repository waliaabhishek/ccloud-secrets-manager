from dataclasses import dataclass

import ccloud_managers.api_key_manager as api_keys
import ccloud_managers.clusters as clusters
import ccloud_managers.service_account as service_accounts


@dataclass(kw_only=True)
class CCloudConfigBundle:
    cc_environments: clusters.CCloudEnvironmentList
    cc_clusters: clusters.CCloudClusterList
    cc_service_accounts: service_accounts.CCloudServiceAccountList
    cc_api_keys: api_keys.CCloudAPIKeyList
