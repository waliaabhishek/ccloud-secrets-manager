import argparse

from app_managers.helpers import printline
import app_managers.workflow_manager.main as WorkflowManager

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application",
        add_help=True,
    )

    conf_args = parser.add_argument_group(
        "configuration-args", "Configuration Arguments for running in CI/CD flow invocation"
    )
    conf_args.add_argument(
        "--csm-config-file-path",
        type=str,
        default=None,
        metavar="/full/path/of/the/configuration/file.yaml",
        help="This is the configuration file path that will provide the connectivity and other config details.",
    )
    conf_args.add_argument(
        "--csm-definitions-file-path",
        type=str,
        default=None,
        metavar="/full/path/of/the/definitions/file.yaml",
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )
    conf_args.add_argument(
        "--csm-generate-definitions-file",
        default=False,
        action="store_true",
        help="This switch can be used for the initial runs where the team does not have a definitions file and would like to auto generate one from the existing ccloud resource mappings.",
    )
    conf_args.add_argument(
        "--dry-run",
        default=False,
        action="store_true",
        help="This switch can be used to invoke a dry run and list all the action that will be preformed, but not performing them.",
    )
    conf_args.add_argument(
        "--disable-api-key-creation",
        default=False,
        action="store_true",
        help="This switch can be used to disable API Key & Secret creation (if required)",
    )
    conf_args.add_argument(
        "--print-delete-eligible-api-keys",
        default=False,
        action="store_true",
        help="This switch can be used to print the API keys which are not synced to the Secret store and (potentially) not used.",
    )

    args = parser.parse_args()

    printline()
    # Trigger Workflows
    WorkflowManager.trigger_workflows(args=args)
    printline()
