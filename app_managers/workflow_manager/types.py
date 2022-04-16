from dataclasses import dataclass, field
from enum import Enum
from typing import Set
import app_managers.core.types as CoreTypes
from ccloud_managers.types import CCloudConfigBundle


class CSMConfigTaskStatus(Enum):
    sts_not_started = "Not Started"
    sts_in_progress = "In Progress"
    sts_success = "Success"
    sts_failed = "Failed"


class CSMConfigTaskType(Enum):
    create_task = "create"
    update_task = "update"
    delete_task = "delete"


class CSMConfigObjectType(Enum):
    sa_type = "service-account"
    api_key_type = "api-key"
    secret_store_type = "secrets-store"
    secret_tags_type = "secret-tags"
    rest_proxy_user_type = "rest-proxy-user"


@dataclass
class CSMConfigTask:
    task_type: str | CSMConfigTaskType
    object_type: str | CSMConfigObjectType
    status: str | CSMConfigTaskStatus
    status_message: str = field(default="Waiting to start")
    task_object: dict = field(default_factory=dict)

    def print_task_data(self):
        print(
            "{:<10} {:<17} {:<15} {:<30} {:<50}".format(
                self.task_type.value,
                self.object_type.value,
                self.status.value,
                self.status_message,
                str(self.task_object),
            )
        )

    def set_task_status(self, task_status, status_msg: str, object_payload: dict = None):
        self.status = task_status
        self.status_message = status_msg
        if object_payload:
            self.task_object = object_payload
        self.print_task_data()


class CSMConfigDataMap:
    csm_bundle: CoreTypes.CSMYAMLConfigBundle
    ccloud_bundle: CCloudConfigBundle

    def __init__(self, csm_bundle: CoreTypes.CSMYAMLConfigBundle, ccloud_bundle: CCloudConfigBundle) -> None:
        self.csm_bundle = csm_bundle
        self.ccloud_bundle = ccloud_bundle

    def find_items_to_be_created(self, config_item_names: set[str], ccloud_item_names: set[str]) -> Set[str]:
        return set(config_item_names.difference(ccloud_item_names))

    def find_items_to_be_deleted(self, config_item_names: set[str], ccloud_item_names: set[str]) -> Set[str]:
        return set(ccloud_item_names.difference(config_item_names))

    def find_common_items(self, config_item_names: set[str], ccloud_item_names: set[str]) -> Set[str]:
        return set(config_item_names.intersection(ccloud_item_names))
