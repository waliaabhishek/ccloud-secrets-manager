from requests.auth import HTTPBasicAuth
from os import environ

CCLOUD_URL = "https://api.confluent.cloud"
URI_LIST = {}
CCLOUD_API_KEY = ""
CCLOUD_API_SECRET = ""
BASIC_AUTH = ""


def initial_setup():
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
