from typing import Set

import app_managers.core.types as CoreTypes
import app_managers.workflow_manager.types as WorkflowTypes
from ccloud_managers.types import CCloudConfigBundle
from secret_managers.types import CSMSecretsManager


class CSMServiceAccountTasks(WorkflowTypes.CSMConfigDataMap):
    sa_in_def: Set[str]
    sa_in_ccloud: Set[str]

    def __init__(self, csm_bundle: CoreTypes.CSMYAMLConfigBundle, ccloud_bundle: CCloudConfigBundle) -> None:
        super().__init__(csm_bundle=csm_bundle, ccloud_bundle=ccloud_bundle)
        self.refresh_set_values(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)

    def refresh_set_values(self, csm_bundle: CoreTypes.CSMYAMLConfigBundle, ccloud_bundle: CCloudConfigBundle):
        self.sa_in_def = set([v.name for v in csm_bundle.csm_definitions.sa])
        self.sa_in_ccloud = set([v.name for v in ccloud_bundle.cc_service_accounts.sa.values()])

    def create_service_account_tasks(self):
        req = self.find_items_to_be_created(self.sa_in_def, self.sa_in_ccloud)
        for item in req:
            sa = self.csm_bundle.csm_definitions.find_service_account(item)
            if sa:
                yield WorkflowTypes.CSMConfigTask(
                    task_type=WorkflowTypes.CSMConfigTaskType.create_task,
                    object_type=WorkflowTypes.CSMConfigObjectType.sa_type,
                    status=WorkflowTypes.CSMConfigTaskStatus.sts_not_started,
                    task_object={"sa_name": sa.name, "description": sa.description},
                )

    def delete_service_account_tasks(self):
        ignore_sa_id_set = set(self.csm_bundle.csm_configs.ccloud.ignore_service_account_list)
        ignore_sa_names = set(
            [
                item.name
                for item in self.ccloud_bundle.cc_service_accounts.sa.values()
                if item.resource_id in ignore_sa_id_set
            ]
        )
        req = self.find_items_to_be_deleted(self.sa_in_def, self.sa_in_ccloud)
        req = self.find_items_to_be_deleted(config_item_names=ignore_sa_names, ccloud_item_names=req)
        for item in req:
            yield WorkflowTypes.CSMConfigTask(
                task_type=WorkflowTypes.CSMConfigTaskType.delete_task,
                object_type=WorkflowTypes.CSMConfigObjectType.sa_type,
                status=WorkflowTypes.CSMConfigTaskStatus.sts_not_started,
                task_object={"sa_name": item},
            )


class CSMAPIKeyTasks(WorkflowTypes.CSMConfigDataMap):
    # The set is a combined string with the format SA.name~ClusterID
    api_keys_in_def: Set[str] = set()
    api_keys_in_ccloud: Set[str] = set()
    create_secrets_req: Set[str] = set()
    update_secrets_req: Set[str] = set()
    secret_bundle: CSMSecretsManager

    def __init__(
        self,
        csm_bundle: CoreTypes.CSMYAMLConfigBundle,
        ccloud_bundle: CCloudConfigBundle,
        secret_bundle: CSMSecretsManager,
    ) -> None:
        super().__init__(csm_bundle=csm_bundle, ccloud_bundle=ccloud_bundle)
        self.secret_bundle = secret_bundle
        self.refresh_set_values(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)

    def refresh_set_values(self, csm_bundle: CoreTypes.CSMYAMLConfigBundle, ccloud_bundle: CCloudConfigBundle):
        for sa in csm_bundle.csm_definitions.sa:
            if "FORCE_ALL_CLUSTERS" in sa.cluster_list:
                self.api_keys_in_def.update(
                    ["~".join([sa.name, v.cluster_id]) for v in ccloud_bundle.cc_clusters.cluster.values()]
                )
            else:
                self.api_keys_in_def.update(["~".join([sa.name, v]) for v in sa.cluster_list])
            sa_id = ccloud_bundle.cc_service_accounts.find_sa(sa.name)
            self.api_keys_in_ccloud.update(
                [
                    "~".join([sa.name, v.cluster_id])
                    for v in ccloud_bundle.cc_api_keys.find_keys_with_sa(getattr(sa_id, "resource_id", None))
                ]
            )

    def create_api_key_tasks(self):
        secrets_in_store = set()
        create_api_keys_req = self.find_items_to_be_created(
            config_item_names=self.api_keys_in_def, ccloud_item_names=self.api_keys_in_ccloud
        )
        secrets_in_store.update(["~".join([v.sa_name, v.cluster_id]) for v in self.secret_bundle.secret.values()])
        self.create_secrets_req = self.find_items_to_be_created(
            config_item_names=self.api_keys_in_def, ccloud_item_names=secrets_in_store
        )
        self.update_secrets_req = create_api_keys_req.intersection(secrets_in_store)
        # This is needed if the Secret does not exist but an API key exists for the cluster.
        # As the secret cannot be retrieved after the first time its created, there is no way
        # to inject the secret to a Secret store in case of any failures. The API Key will need
        # to be freshly created and synced to the Secret Store.
        force_api_key_create = self.find_items_to_be_created(
            config_item_names=self.create_secrets_req, ccloud_item_names=create_api_keys_req
        )
        create_api_keys_req.update(force_api_key_create)
        for item in create_api_keys_req:
            value = item.split("~", 1)
            sa_name, cluster_id = value[0], value[1]
            cluster_details = self.ccloud_bundle.cc_clusters.find_cluster(cluster_id)
            # sa_details = self.ccloud_bundle.cc_service_accounts.find_sa(sa_name)
            # api_keys = self.ccloud_bundle.cc_api_keys.find_keys_with_sa_and_cluster(
            #     getattr(sa_details, "resource_id", None), cluster_details.cluster_id
            # )
            yield WorkflowTypes.CSMConfigTask(
                task_type=WorkflowTypes.CSMConfigTaskType.create_task,
                object_type=WorkflowTypes.CSMConfigObjectType.api_key_type,
                status=WorkflowTypes.CSMConfigTaskStatus.sts_not_started,
                task_object={
                    "sa_name": sa_name,
                    "cluster_id": cluster_details.cluster_id,
                    "env_id": cluster_details.env_id,
                },
            )

    def delete_api_key_tasks(self):
        ignore_sa_id_set = set(self.csm_bundle.csm_configs.ccloud.ignore_service_account_list)
        ignore_sa_names = set(
            [
                item.name
                for item in self.ccloud_bundle.cc_service_accounts.sa.values()
                if item.resource_id in ignore_sa_id_set
            ]
        )
        deletion_eligible_api_keys = self.find_items_to_be_deleted(
            config_item_names=self.api_keys_in_def, ccloud_item_names=self.api_keys_in_ccloud
        )
        deletion_eligible_api_keys = set(
            [item for item in deletion_eligible_api_keys if item.split("~", 1)[0] not in ignore_sa_names]
        )
        for item in deletion_eligible_api_keys:
            value = item.split("~", 1)
            sa_name, cluster_id = value[0], value[1]
            sa_id = self.ccloud_bundle.cc_service_accounts.find_sa(sa_name=sa_name)
            for key in self.ccloud_bundle.cc_api_keys.find_keys_with_sa_and_cluster(
                sa_id=sa_id, cluster_id=cluster_id
            ):
                yield WorkflowTypes.CSMConfigTask(
                    task_type=WorkflowTypes.CSMConfigTaskType.delete_task,
                    object_type=WorkflowTypes.CSMConfigObjectType.api_key_type,
                    status=WorkflowTypes.CSMConfigTaskStatus.sts_not_started,
                    task_object={"sa_name": sa_name, "sa_id": sa_id, "cluster_id": cluster_id, "api_key": key.api_key},
                )


class CSMSecretManagerTasks(WorkflowTypes.CSMConfigDataMap):
    create_secrets_req: Set[str]
    update_secrets_req: Set[str]
    api_key_tasks: CSMAPIKeyTasks

    def __init__(
        self,
        csm_bundle: CoreTypes.CSMYAMLConfigBundle,
        ccloud_bundle: CCloudConfigBundle,
        api_key_tasks: CSMAPIKeyTasks,
    ) -> None:
        super().__init__(csm_bundle=csm_bundle, ccloud_bundle=ccloud_bundle)
        self.api_key_tasks = api_key_tasks
        self.refresh_set_values(self.api_key_tasks)

    def refresh_set_values(self, api_key_tasks: CSMAPIKeyTasks):
        self.create_secrets_req = api_key_tasks.create_secrets_req
        self.update_secrets_req = api_key_tasks.update_secrets_req

    def create_secret_tasks(self):
        for item in self.create_secrets_req:
            value = item.split("~", 1)
            sa_name, cluster_id = value[0], value[1]
            cluster_details = self.ccloud_bundle.cc_clusters.find_cluster(cluster_id)
            sa_definition = self.csm_bundle.csm_definitions.find_service_account(sa_name)
            yield WorkflowTypes.CSMConfigTask(
                task_type=WorkflowTypes.CSMConfigTaskType.create_task,
                object_type=WorkflowTypes.CSMConfigObjectType.secret_store_type,
                status=WorkflowTypes.CSMConfigTaskStatus.sts_not_started,
                task_object={
                    "sa_name": sa_name,
                    "cluster_id": cluster_id,
                    "env_id": cluster_details.env_id,
                    "need_rp_access": sa_definition.rp_access,
                    "is_rp_user": sa_definition.is_rp_user,
                },
            )

    def update_secret_tasks(self):
        for item in self.update_secrets_req:
            value = item.split("~", 1)
            sa_name, cluster_id = value[0], value[1]
            cluster_details = self.ccloud_bundle.cc_clusters.find_cluster(cluster_id)
            sa_definition = self.csm_bundle.csm_definitions.find_service_account(sa_name)
            yield WorkflowTypes.CSMConfigTask(
                task_type=WorkflowTypes.CSMConfigTaskType.update_task,
                object_type=WorkflowTypes.CSMConfigObjectType.secret_store_type,
                status=WorkflowTypes.CSMConfigTaskStatus.sts_not_started,
                task_object={
                    "sa_name": sa_name,
                    "cluster_id": cluster_id,
                    "env_id": cluster_details.env_id,
                    "need_rp_access": sa_definition.rp_access,
                    "is_rp_user": sa_definition.is_rp_user,
                },
            )
