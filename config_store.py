from enum import Enum
from typing import OrderedDict

import data_parser
from api_key_manager import CCloudAPIKeyList
from clusters import CCloudClusterList
from service_account import CCloudServiceAccountList


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
    aws_secret_type = "aws-secrets"


class CSMConfigTask:
    def __init__(
        self, task_type, object_type, status, task_object: dict, status_msg: str = "Waiting to start"
    ) -> None:
        self.task_type = task_type
        self.object_type = object_type
        self.status = status
        self.status_message = status_msg
        self.task_object = task_object


class CSMConfigDataMap:
    tasks: OrderedDict[int, CSMConfigTask]

    def __init__(self) -> None:
        self.tasks = {}

    def add_new_task(
        self, task_type, object_type, status, task_object: dict, status_msg: str = "Waiting to start"
    ) -> bool:
        self.tasks[len(self.tasks)] = CSMConfigTask(task_type, object_type, status, task_object, status_msg)

    def set_task_status(
        self, task_key: int, task_status, status_msg: str, object_payload: dict = None
    ) -> CSMConfigTask:
        self.tasks[task_key].status = task_status
        self.tasks[task_key].status_message = status_msg
        if object_payload:
            self.tasks[task_key].task_object = object_payload
        self.print_task_data(task_key, self.tasks[task_key])
        return self.tasks[task_key]

    def select_new_tasks(self):
        for k, v in self.tasks.items():
            if v.status == CSMConfigTaskStatus.sts_not_started:
                yield k, v

    def populate_data_map(
        self,
        definitions: data_parser.CSMDefinitions,
        ccloud_sa_details: CCloudServiceAccountList,
        ccloud_api_key_details: CCloudAPIKeyList,
        ccloud_clusters: CCloudClusterList,
        csm_configs: data_parser.CSMConfig,
    ):
        #  Analyse and set up tasks for Service Account management
        sa_in_def = set([v.name for v in definitions.sa])
        sa_in_ccloud = set([v.name for v in ccloud_sa_details.sa.values()])
        # both_present = set(sa_in_def).intersection(sa_in_ccloud)
        create_req = set(sa_in_def).difference(sa_in_ccloud)
        delete_req = set(sa_in_ccloud).difference(sa_in_def)
        for item in create_req:
            sa = definitions.find_service_account(item)
            if sa:
                self.add_new_task(
                    CSMConfigTaskType.create_task,
                    CSMConfigObjectType.sa_type,
                    CSMConfigTaskStatus.sts_not_started,
                    {"sa_name": sa.name, "description": sa.description},
                )
        if csm_configs.ccloud.enable_sa_cleanup:
            for item in delete_req:
                # sa = definitions.find_service_account(item)
                # if not sa:
                self.add_new_task(
                    CSMConfigTaskType.delete_task,
                    CSMConfigObjectType.sa_type,
                    CSMConfigTaskStatus.sts_not_started,
                    {"sa_name": item},
                )
        # Analyse and setup tasks for API Key management
        # cluster_list = set(
        #     [v.cluster_id for v in ccloud_clusters.cluster.values()])
        for sa in definitions.sa:
            if "FORCE_ALL_CLUSTERS" in sa.cluster_list:
                for cluster in ccloud_clusters.cluster.values():
                    api_keys = ccloud_api_key_details.find_keys_with_sa_and_cluster(sa.name, cluster.cluster_id)
                    if not api_keys:
                        self.add_new_task(
                            CSMConfigTaskType.create_task,
                            CSMConfigObjectType.api_key_type,
                            CSMConfigTaskStatus.sts_not_started,
                            {"sa_name": sa.name, "cluster_id": cluster.cluster_id, "env_id": cluster.env_id},
                        )
                        self.add_new_task(
                            CSMConfigTaskType.create_task,
                            CSMConfigObjectType.aws_secret_type,
                            CSMConfigTaskStatus.sts_not_started,
                            {"sa_name": sa.name, "cluster_id": cluster.cluster_id, "env_id": cluster.env_id},
                        )
            else:
                for cluster in sa.cluster_list:
                    api_keys = ccloud_api_key_details.find_keys_with_sa_and_cluster(sa.name, cluster)
                    if not api_keys:
                        temp_cluster = ccloud_clusters.find_cluster(cluster)
                        self.add_new_task(
                            CSMConfigTaskType.create_task,
                            CSMConfigObjectType.api_key_type,
                            CSMConfigTaskStatus.sts_not_started,
                            {"sa_name": sa.name, "cluster_id": cluster, "env_id": temp_cluster.env_id},
                        )
                        self.add_new_task(
                            CSMConfigTaskType.create_task,
                            CSMConfigObjectType.aws_secret_type,
                            CSMConfigTaskStatus.sts_not_started,
                            {"sa_name": sa.name, "cluster_id": cluster, "env_id": temp_cluster.env_id},
                        )

    def print_data_map(self, include_create: bool = True, include_delete: bool = True):
        print("=" * 80)
        print(
            "{:<3} {:<10} {:<17} {:<15} {:<30} {:<50}".format(
                "S# ", "Task Type", "Object Type", "Current Status", "Status Message", "Object Payload"
            )
        )
        for k, v in self.tasks.items():
            if (v.task_type == CSMConfigTaskType.create_task and include_create) or (
                v.task_type == CSMConfigTaskType.delete_task and include_delete
            ):
                print(
                    "{:<3} {:<10} {:<17} {:<15} {:<30} {:<50}".format(
                        k, v.task_type.value, v.object_type.value, v.status.value, v.status_message, str(v.task_object)
                    )
                )
        print("=" * 80)

    def print_task_data(self, task_key: int, task_object: CSMConfigTask):
        print(
            "{:<3} {:<10} {:<17} {:<15} {:<30} {:<50}".format(
                task_key,
                task_object.task_type.value,
                task_object.object_type.value,
                task_object.status.value,
                task_object.status_message,
                str(task_object.task_object),
            )
        )
