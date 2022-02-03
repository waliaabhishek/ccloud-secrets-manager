from typing import Dict
from urllib import parse

import requests

import base_ccloud


class CCloudEnvironment:
    def __init__(self, env_id: str, display_name: str, created_at: str) -> None:
        self.env_id = env_id
        self.display_name = display_name
        self.created_at = created_at


class CCloudEnvironmentList:
    env: Dict[str, CCloudEnvironment]

    def __init__(self, ccloud_connection: base_ccloud.CCloudConnection) -> None:
        uri = base_ccloud.URIDetails()
        self.env_url = uri.get_endpoint_url(uri.environments)
        self.auth = ccloud_connection.api_http_basic_auth
        self.env = {}
        print("Gathering list of all environment(s) in CCloud.")
        self.read_all_env()

    def __str__(self):
        print("Found " + str(len(self.env)) + " environments.")
        for v in self.env.values():
            print("{:<15} {:<40}".format(v.env_id, v.display_name))

    def read_all_env(self, params={"page_size": 50}):
        resp = requests.get(url=self.env_url, auth=self.auth, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            for item in out_json["data"]:
                print("Found environment " + item["id"] + " with name " + item["display_name"])
                self.__add_env_to_cache(item["id"], item["display_name"], item["metadata"]["created_at"])
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all_env(params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_env_to_cache(self, env_id: str, display_name: str, created_at: str) -> CCloudEnvironment:
        self.env[env_id] = CCloudEnvironment(env_id, display_name, created_at)
        return self.env[env_id]

    # Read/Find one Cluster from the cache
    def find_environment(self, env_id):
        return self.env[env_id]


class CCloudCluster:
    def __init__(
        self,
        env_id: str,
        cluster_id: str,
        cluster_name: str,
        cloud: str,
        availability: str,
        region: str,
        bootstrap_url: str,
    ) -> None:
        self.env_id = env_id
        self.cluster_id = cluster_id
        self.cluster_name = cluster_name
        self.cloud = cloud
        self.availability = availability
        self.region = region
        self.bootstrap_url = bootstrap_url


class CCloudClusterList:
    cluster: Dict[str, CCloudCluster]

    def __init__(self, ccloud_connection: base_ccloud.CCloudConnection, ccloud_env: CCloudEnvironmentList) -> None:
        uri = base_ccloud.URIDetails()
        self.cluster_url = uri.get_endpoint_url(uri.clusters)
        self.auth = ccloud_connection.api_http_basic_auth
        self.cluster = {}
        print("Gathering list of all Cluster(s) in every environment in CCloud.")
        for item in ccloud_env.env.values():
            print("Checking Environment " + item.env_id + " for any provisioned clusters.")
            self.read_all_clusters(item.env_id)

    def __str__(self):
        for v in self.cluster.values():
            print(
                "{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
                    v.env_id, v.cluster_id, v.cluster_name, v.cloud, v.availability, v.bootstrap_url
                )
            )

    def read_all_clusters(self, env_id: str, params={"page_size": 50}):
        params["environment"] = env_id
        resp = requests.get(url=self.cluster_url, auth=self.auth, params=params)
        if resp.status_code == 200:
            out_json = resp.json()
            for item in out_json["data"]:
                print("Found cluster " + item["id"] + " with name " + item["spec"]["display_name"])
                self.__add_cluster_to_cache(
                    env_id,
                    item["id"],
                    item["spec"]["display_name"],
                    item["spec"]["cloud"],
                    item["spec"]["availability"],
                    item["spec"]["region"],
                    item["spec"]["kafka_bootstrap_endpoint"],
                )
            if "next" in out_json["metadata"]:
                query_params = parse.parse_qs(parse.urlsplit(out_json["metadata"]["next"]).query)
                params["page_token"] = str(query_params["page_token"][0])
                self.read_all_clusters(env_id, params)
        else:
            raise Exception("Could not connect to Confluent Cloud. Please check your settings. " + resp.text)

    def __add_cluster_to_cache(
        self,
        env_id: str,
        cluster_id: str,
        cluster_name: str,
        cloud: str,
        availability: str,
        region: str,
        bootstrap_url: str,
    ) -> CCloudCluster:
        self.cluster[cluster_id] = CCloudCluster(
            env_id, cluster_id, cluster_name, cloud, availability, region, bootstrap_url
        )
        return self.cluster[cluster_id]

    # Read/Find one Cluster from the cache
    def find_cluster(self, cluster_id):
        return self.cluster[cluster_id]
