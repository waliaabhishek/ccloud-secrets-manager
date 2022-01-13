import argparse
import base_ccloud
import service_account
import api_key_manager

parser = argparse.ArgumentParser(
    description="Command line arguments for controlling the application", add_help=True, )

wf_args = parser.add_argument_group(
    "workflow-args", "Workflow Selection Arguments")
wf_args.add_argument('--wf-name', type=str, default=[], action="append", dest="wf_name",
                     choices=['all', 'create-sa', 'get-sa', 'create-apikey'], help='Workflow that needs to be executed')

sa_args = parser.add_argument_group("sa-args", "Service Account Arguments")
sa_args.add_argument('--service-account-name', type=str, default=None,
                     help="Provide the name for which Service Account needs to be created.",)
sa_args.add_argument('--force-new-account', action="store_true", default=False,
                     help="Force Generate a new Service Account even if an account exists with the same Name.",)
sa_args.add_argument('--add-as-rest-proxy-user', action="store_true", default=False,
                     help="Add the Service Account to REST Proxy users",)

api_args = parser.add_argument_group("api-args", "API management arguments")
api_args.add_argument('--setup-api-keys-for-clusters', action="store_true", default=False,
                      help="Generate new API Keys & Secrets while setting up the new Service Account",)
api_args.add_argument('--create-service-account-if-necessary', action="store_true", default=False,
                      help="If enabled, will create a new Service Account when trying to create a new API Key. " +
                      "Otherwise, will throw an exception if Service Account is not found prior to creating the API Key.",)
api_args.add_argument('--cluster-id', type=str, metavar="lkc-123456", default=[], action="append", dest="cluster_list",
                      help='The Clusters list for which the api keys will be set up. ')
api_args.add_argument('--force-all-clusters', action="store_true", default=False,
                      help="If the API keys are needed for all available clusters, then --cluster-id could be ignored and this switch could be enabled.",)
api_args.add_argument('--force-new-api-keys', action="store_true", default=False,
                      help="This will generate new API Keys even if there are existing API keys linked to a cluster already.",)
api_args.add_argument('--generate-api-keys-output-file', action="store_true", default=False,
                      help="This will generate new API Keys even if there are existing API keys linked to a cluster already.",)

sm_args = parser.add_argument_group("sm-args", "Secret Manager arguments")
sm_args.add_argument('--enable-secret-management', action="store_true", default=False,
                     help="This switch enables the Secret Management for API Keys and Service Accounts.",)
sm_args.add_argument('--secret-manager-type', type=str, default="aws-secretmanager",
                     choices=['aws-secretmanager'],
                     help="Provide the Secret Storage Type to use",)
sm_args.add_argument('--secret-name-prefix', type=str, metavar="prefix_for_secret_name", default=None,
                     help='If you need to add a prefix to the Secret name, it could be done with this switch.')

args = parser.parse_args()

base_ccloud.initial_setup(args.setup_api_keys_for_clusters)
# run_api_key_workflow(args.service_account_name, args.force_new_account, args.setup_api_keys_for_clusters, args.cluster_list,
#                      args.force_all_clusters, args.force_new_api_keys, args.generate_api_keys_output_file)

sa_output, existing_api_key_output, new_api_key_output = {}, {}, {}

if "create-sa" in args.wf_name:
    sa_output = service_account.run_sa_workflow(
        args.service_account_name, True, args.force_new_account)
if "get-sa" in args.wf_name:
    sa_output = service_account.run_sa_workflow(
        args.service_account_name, False, False)
if "create-apikey" in args.wf_name:
    existing_api_key_output, new_api_key_output = api_key_manager.run_api_key_workflow(
        args.service_account_name, args.create_service_account_if_necessary, True,
        args.cluster_list, args.force_all_clusters, args.force_new_api_keys, True)
    if new_api_key_output and args.enable_secret_management:
        if "aws-secretmanager" == args.secret_manager_type:
            import aws_secrets_manager
            print("=" * 40)
            aws_secrets_manager.run_aws_sec_mgr_workflow(
                "store", new_api_key_output, args.add_as_rest_proxy_user, None)
print("")
