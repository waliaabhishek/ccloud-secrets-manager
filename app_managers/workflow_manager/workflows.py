import itertools
from dataclasses import dataclass, field

import app_managers.core.types as CoreTypes
from app_managers.workflow_manager.task_generator import CSMAPIKeyTasks, CSMSecretManagerTasks, CSMServiceAccountTasks
from app_managers.workflow_manager.types import CSMConfigTaskStatus
from ccloud_managers.types import CCloudConfigBundle
from secret_managers.types import CSMSecretsManager


@dataclass(kw_only=True)
class WorkflowManager:
    csm_bundle: CoreTypes.CSMYAMLConfigBundle
    ccloud_bundle: CCloudConfigBundle
    secret_bundle: CSMSecretsManager
    dry_run: bool
    sa_tasks: CSMServiceAccountTasks = field(init=False)
    api_key_tasks: CSMAPIKeyTasks = field(init=False)
    secret_tasks: CSMSecretManagerTasks = field(init=False)

    def __post_init__(self) -> None:
        self.sa_tasks = CSMServiceAccountTasks(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)
        self.api_key_tasks = CSMAPIKeyTasks(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)
        self.secret_tasks = CSMSecretManagerTasks(
            csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle, api_key_tasks=self.api_key_tasks
        )

    def create_service_accounts(self):
        print("Triggering Service Account creation Workflow")
        self.sa_tasks.refresh_set_values(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)
        for item in self.sa_tasks.create_service_account_tasks():
            item.print_task_data()
            if not self.dry_run:
                new_sa, is_success = self.ccloud_bundle.cc_service_accounts.create_sa(
                    sa_name=item.task_object["sa_name"],
                    description=item.task_object["description"],
                )
                if is_success:
                    item.set_task_status(
                        task_status=CSMConfigTaskStatus.sts_success,
                        status_msg="Service Account Creation Succeeded.",
                        object_payload={"sa_id": new_sa.resource_id, "sa_name": new_sa.name},
                    )

    def delete_service_accounts(self):
        print("Triggering Service Account deletion Workflow")
        self.sa_tasks.refresh_set_values(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)
        for item in self.sa_tasks.delete_service_account_tasks():
            item.print_task_data()
            if not self.dry_run:
                sa_id = self.ccloud_bundle.cc_service_accounts.find_sa(item.task_object["sa_name"]).resource_id
                is_success = self.ccloud_bundle.cc_service_accounts.delete_sa(item.task_object["sa_name"])
                if is_success:
                    item.set_task_status(
                        task_status=CSMConfigTaskStatus.sts_success,
                        status_msg="Service Account deletion Succeeded.",
                        object_payload={"sa_id": sa_id, "sa_name": item.task_object["sa_name"]},
                    )

    def create_api_keys(self):
        print("Triggering API Key creation workflow")
        self.api_key_tasks.refresh_set_values(csm_bundle=self.csm_bundle, ccloud_bundle=self.ccloud_bundle)
        for item in self.api_key_tasks.create_api_key_tasks():
            item.print_task_data()
            if not self.dry_run:
                sa_details = self.ccloud_bundle.cc_service_accounts.find_sa(item.task_object["sa_name"])
                new_api_key, is_success = self.ccloud_bundle.cc_api_keys.create_api_key(
                    env_id=item.task_object["env_id"],
                    cluster_id=item.task_object["cluster_id"],
                    sa_id=sa_details.resource_id,
                    sa_name=sa_details.name,
                    description=f"API Key for sa {sa_details.resource_id} created by the CI/CD workflow",
                )
                if is_success:
                    item.set_task_status(
                        task_status=CSMConfigTaskStatus.sts_success,
                        status_msg="API Key creation succeeded.",
                        object_payload={"api_key": new_api_key["key"], "env_id": item.task_object["env_id"]},
                    )

    def update_api_keys_in_secret_manager(self) -> bool:
        is_secret_updated = False
        print("Triggering Secret Manager Update workflow")
        self.secret_tasks.refresh_set_values(api_key_tasks=self.api_key_tasks)
        for item in itertools.chain(self.secret_tasks.create_secret_tasks(), self.secret_tasks.update_secret_tasks()):
            item.print_task_data()
            if not self.dry_run:
                sa_details = self.ccloud_bundle.cc_service_accounts.find_sa(item.task_object["sa_name"])
                api_key_details = self.ccloud_bundle.cc_api_keys.find_keys_with_sa_and_cluster(
                    sa_details.resource_id, item.task_object["cluster_id"]
                )
                for api_key in api_key_details:
                    if api_key.api_secret:
                        self.secret_bundle.create_or_update_secret(
                            api_key=api_key,
                            ccloud_bundle=self.ccloud_bundle,
                            csm_bundle=self.csm_bundle,
                        )
                    is_secret_updated = True
        return is_secret_updated
