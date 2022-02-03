from json import dumps
from typing import Dict, Tuple
from urllib import parse

import requests

import base_ccloud
from helpers import pretty as pp

SA_VALUE = {}


class CCloudServiceAccount:
    def __init__(self, res_id: str, name: str, description: str, created_at, updated_at) -> None:
        self.resource_id = res_id
        self.name = name
        self.description = description
        self.created_at = created_at
        self.updated_at = updated_at


class CCloudServiceAccountList:

    sa: Dict[str, CCloudServiceAccount]

    def __init__(self, ccloud_connection: base_ccloud.CCloudConnection) -> None:
        uri = base_ccloud.URIDetails()
        self.sa_url = uri.get_endpoint_url(uri.service_accounts)
        self.auth = ccloud_connection.api_http_basic_auth
        self.sa = {}
        print("Gathering list of all Service Account(s) in CCloud.")
        self.read_all_sa({"page_size": 50})

    def __str__(self) -> str:
        for item in self.sa.values():
            print("{:<15} {:<40} {:<50}".format(item.resource_id, item.name, item.description))

    # Read ALL Service Account details from Confluent Cloud
    def read_all_sa(self, params):
        resp = requests.get(url=self.sa_url, auth=self.auth, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            for item in out_json["data"]:
                print("Found Service Account " + item["id"] + " with name " + item["display_name"])
                self.add_to_cache(
                    item["id"],
                    item["display_name"],
                    item["description"],
                    item["metadata"]["created_at"],
                    item["metadata"]["updated_at"],
                )
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all_sa(params)
            # pp.pprint(out_json)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def add_to_cache(self, res_id: str, name: str, description: str, created_at, updated_at) -> CCloudServiceAccount:
        self.sa[res_id] = CCloudServiceAccount(res_id, name, description, created_at, updated_at)
        return self.sa[res_id]

    # Read/Find one SA from the cache
    def find_sa(self, sa_name):
        for item in self.sa.values():
            if sa_name == item.name:
                return item
        return None

    def delete_from_cache(self, res_id):
        self.sa.pop(res_id, None)

    # Create/Find one SA and add it to the cache, so that we do not have to refresh the cache manually
    def create_sa(self, sa_name, description=None) -> Tuple[CCloudServiceAccount, bool]:
        temp = self.find_sa(sa_name)
        if temp:
            return temp, False
        # print("Creating a new Service Account with name: " + sa_name)
        payload = {
            "display_name": sa_name,
            "description": str("Account for " + sa_name + " created by CI/CD framework")
            if not description
            else description,
        }
        resp = requests.post(
            url=self.sa_url,
            auth=self.auth,
            json=payload,
        )
        if resp.status_code == 201:
            sa_details = resp.json()
            # pp.pprint(sa_details)
            return (
                self.add_to_cache(
                    sa_details["id"],
                    sa_details["display_name"],
                    sa_details["description"],
                    sa_details["metadata"]["created_at"],
                    sa_details["metadata"]["updated_at"],
                ),
                True,
            )
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def delete_sa(self, sa_name) -> bool:
        temp = self.find_sa(sa_name)
        if not temp:
            print("Did not find Service Account with name '" + sa_name + "'. Not deleting anything.")
            return False
        else:
            resp = requests.delete(url=str(self.sa_url + "/" + temp.resource_id), auth=self.auth)
            if resp.status_code == 204:
                self.delete_from_cache(temp.resource_id)
                return True
            else:
                raise Exception("Could not perform the DELETE operation. Please check your settings. " + resp.text)
