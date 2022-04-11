from argparse import Namespace

import app_managers.core.initializers as CSMInit
import app_managers.core.types as CSMTypes
import ccloud_managers.api_key_reconciliation as ApiKeyReconciliation
import ccloud_managers.initializers as CCloudInit
from app_managers.workflow_manager.workflows import WorkflowManager

import app_managers.workflow_manager.generate_definitions as DefinitionsGenerator


def trigger_workflows(args: Namespace):
    # parse the YAML files for the input configurations
    csm_bundle = CSMInit.load_parse_yamls(
        args.csm_config_file_path, args.csm_definitions_file_path, args.csm_generate_definitions_file
    )

    # Initialize CCloud Object Cache
    ccloud_bundle = CCloudInit.load_parse_yamls(csm_bundle=csm_bundle)

    # If the Generate YAML is True, we will parse the data and render a YAML file
    if args.csm_generate_definitions_file:
        DefinitionsGenerator.create_definitions_file(def_file_path="test_output.yaml", ccloud_bundle=ccloud_bundle)
    # This path will only get executed if the YAML files is passed in and
    # Generate YAML file is unchecked.
    else:
        if csm_bundle.csm_configs.secretstore.store_type == CSMTypes.SUPPORTED_STORES.AWS_SECRETS:
            import secret_managers.aws_secrets_manager as aws_secrets_manager

            secret_list = aws_secrets_manager.AWSSecretsList(csm_bundle=csm_bundle)
        workflow_manager = WorkflowManager(
            csm_bundle=csm_bundle,
            ccloud_bundle=ccloud_bundle,
            secret_bundle=secret_list,
            dry_run=args.dry_run,
        )
        workflow_manager.create_service_accounts()
        if csm_bundle.csm_configs.ccloud.enable_sa_cleanup:
            workflow_manager.delete_service_accounts()
        if not args.disable_api_key_creation:
            workflow_manager.create_api_keys()
            if args.print_delete_eligible_api_keys:
                delete_eligible_api_keys = ApiKeyReconciliation.find_api_keys_eligible_for_deletion(
                    csm_secret_list=secret_list,
                    cc_api_keys=ccloud_bundle.cc_api_keys,
                    ignored_sa_list=csm_bundle.csm_configs.ccloud.ignore_service_account_list,
                )
                if delete_eligible_api_keys:
                    ccloud_bundle.cc_api_keys.print_api_keys(
                        ccloud_sa=ccloud_bundle.cc_service_accounts, api_keys=delete_eligible_api_keys
                    )
                else:
                    print("No deletion eligible keys detected.")
            is_secret_updated = workflow_manager.update_api_keys_in_secret_manager()
            if is_secret_updated:
                secret_list.create_update_rest_proxy_secrets(ccloud_bundle=ccloud_bundle, csm_bundle=csm_bundle)
