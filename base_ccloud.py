from requests.auth import HTTPBasicAuth
from os import environ, name

CCLOUD_URL = "https://api.confluent.cloud"
URI_LIST = {}
CCLOUD_API_KEY = ""
CCLOUD_API_SECRET = ""
BASIC_AUTH = ""
CONFLUENT_CLOUD_EMAIL = ""
CONFLUENT_CLOUD_PASSWORD = ""


def initial_setup(setup_api_keys_for_clusters: bool):
    global URI_LIST
    URI_LIST = {
        "environment": "/org/v2/environments",
        "sa": "/iam/v2/service-accounts",
        "apikey": "/iam/v2/api-keys",
        "clusters": "/cmk/v2/clusters",
    }

    if environ.get("CCLOUD_API_KEY") is None:
        raise Exception(
            'Please populate CCLOUD_API_KEY with the Cloud resource type API Key value')
    else:
        global CCLOUD_API_KEY
        CCLOUD_API_KEY = environ['CCLOUD_API_KEY']

    if environ.get("CCLOUD_API_SECRET") is None:
        raise Exception(
            'Please populate CCLOUD_API_SECRET with the Cloud resource type API Secret value')
    else:
        global CCLOUD_API_SECRET
        CCLOUD_API_SECRET = environ['CCLOUD_API_SECRET']

    global BASIC_AUTH
    BASIC_AUTH = HTTPBasicAuth(CCLOUD_API_KEY, CCLOUD_API_SECRET)

    if setup_api_keys_for_clusters:
        if environ.get("CONFLUENT_CLOUD_EMAIL") is None:
            raise Exception(
                'Please populate CONFLUENT_CLOUD_EMAIL environment variable with a CCloud Username')
        else:
            global CONFLUENT_CLOUD_EMAIL
            CCLOUD_API_SECRET = environ['CONFLUENT_CLOUD_EMAIL']

        if environ.get("CONFLUENT_CLOUD_PASSWORD") is None:
            raise Exception(
                'Please populate CONFLUENT_CLOUD_PASSWORD environment variable with a CCloud Password')
        else:
            global CONFLUENT_CLOUD_PASSWORD
            CCLOUD_API_SECRET = environ['CONFLUENT_CLOUD_PASSWORD']
