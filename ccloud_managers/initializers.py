from app_managers.core.types import CSMYAMLConfigBundle
from app_managers.helpers import printline

from ccloud_managers.api_key_manager import CCloudAPIKeyList
from ccloud_managers.clusters import CCloudClusterList, CCloudEnvironmentList
from ccloud_managers.connection import CCloudConnection
from ccloud_managers.service_account import CCloudServiceAccountList
from ccloud_managers.types import CCloudConfigBundle


def initialize(csm_bundle: CSMYAMLConfigBundle) -> CCloudConfigBundle:
    ccloud_conn = CCloudConnection(csm_bundle=csm_bundle)
    printline()
    # Gather Environment List from CCloud
    ccloud_env_list = CCloudEnvironmentList(_ccloud_connection=ccloud_conn)
    printline()
    # Gather Cluster ist from al the environments
    ccloud_cluster_list = CCloudClusterList(_ccloud_connection=ccloud_conn, ccloud_env=ccloud_env_list)
    printline()
    # Gather Service Account details pre-existing in CCloud
    ccloud_sa_list = CCloudServiceAccountList(_ccloud_connection=ccloud_conn, _csm_bundle=csm_bundle)
    printline()
    ccloud_api_key_list = CCloudAPIKeyList(_ccloud_connection=ccloud_conn, ccloud_sa=ccloud_sa_list)
    printline()
    ccloud_bundle = CCloudConfigBundle(
        cc_environments=ccloud_env_list,
        cc_clusters=ccloud_cluster_list,
        cc_service_accounts=ccloud_sa_list,
        cc_api_keys=ccloud_api_key_list,
    )
    return ccloud_bundle
