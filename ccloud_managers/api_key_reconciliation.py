from typing import List

from secret_managers.types import CSMSecretsManager

from ccloud_managers.api_key_manager import CCloudAPIKey, CCloudAPIKeyList


def find_api_keys_eligible_for_deletion(
    csm_secret_list: CSMSecretsManager,
    cc_api_keys: CCloudAPIKeyList,
    ignored_sa_list: List[str],
) -> List[CCloudAPIKey]:
    output: List[CCloudAPIKey] = []
    api_keys_in_ccloud = set([k for k in cc_api_keys.api_keys.keys()])
    api_keys_in_secret_store = set([v.api_key for v in csm_secret_list.secret.values()])
    deletion_eligible_api_keys = api_keys_in_ccloud.difference(api_keys_in_secret_store)
    for item in deletion_eligible_api_keys:
        api_key_details = cc_api_keys.api_keys[item]
        if api_key_details.owner_id not in ignored_sa_list:
            output.append(cc_api_keys.api_keys[item])
    return output
