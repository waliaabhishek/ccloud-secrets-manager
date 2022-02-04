from typing import List
from secrets_manager_interface import CSMSecret, CSMSecretsList
from api_key_manager import CCloudAPIKey, CCloudAPIKeyList


def find_api_keys_eligible_for_deletion(
    csm_secret_list: CSMSecretsList,
    ccloud_api_key_list: CCloudAPIKeyList,
    ignored_sa_list: List[str],
) -> List[CCloudAPIKey]:
    output: List[CCloudAPIKey] = []
    api_keys_in_ccloud = set([k for k in ccloud_api_key_list.api_keys.keys()])
    api_keys_in_secret_store = set([v.api_key for v in csm_secret_list.secret.values()])
    deletion_eligible_api_keys = api_keys_in_ccloud.difference(api_keys_in_secret_store)
    for item in deletion_eligible_api_keys:
        api_key_details = ccloud_api_key_list.api_keys[item]
        if api_key_details.owner_id not in ignored_sa_list:
            output.append(ccloud_api_key_list.api_keys[item])
    return output
