from argparse import Namespace

import app_managers.core.initializers as CSMInit
import app_managers.core.types as CSMTypes
import ccloud_managers.initializers as CCloudInit
from app_managers.workflow_manager.workflows import WorkflowManager

import app_managers.workflow_manager.generate_definitions as DefinitionsGenerator


def trigger_workflows(args: Namespace):
    # parse the YAML files for the input configurations
    csm_bundle = CSMInit.initialize(
        args.csm_config_file_path, args.csm_definitions_file_path, args.csm_generate_definitions_file
    )

    # Initialize CCloud Object Cache
    ccloud_bundle = CCloudInit.initialize(csm_bundle=csm_bundle)

    # If the Generate YAML is True, we will parse the data and render a YAML file
    if args.csm_generate_definitions_file:
        DefinitionsGenerator.create_definitions_file(def_file_path="test_output.yaml", ccloud_bundle=ccloud_bundle)
    # This path will only get executed if the YAML files is passed in and
    # Generate YAML file is unchecked.
    else:
        if csm_bundle.csm_configs.secretstore.store_type == CSMTypes.SUPPORTED_STORES.AWS_SECRETS:
            import secret_managers.aws_secrets_manager as aws_secrets_manager

            secret_bundle = aws_secrets_manager.AWSSecretsList(csm_bundle=csm_bundle, ccloud_bundle=ccloud_bundle)
        workflow_manager = WorkflowManager(
            csm_bundle=csm_bundle,
            ccloud_bundle=ccloud_bundle,
            secret_bundle=secret_bundle,
            dry_run=args.dry_run,
        )
        workflow_manager.create_service_accounts()
        if not args.disable_api_key_creation:
            # API Key management workflows
            workflow_manager.create_api_keys()
            if csm_bundle.csm_configs.ccloud.enable_api_key_cleanup:
                workflow_manager.delete_api_keys()

            # Secret management Workflows
            _ = workflow_manager.update_api_keys_in_secret_manager()
            workflow_manager.update_rest_proxy_api_keys_in_secret_manager()
        if csm_bundle.csm_configs.ccloud.enable_sa_cleanup:
            workflow_manager.delete_service_accounts()
