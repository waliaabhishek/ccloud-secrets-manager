import pprint
from urllib import parse

import requests

import base_ccloud

pp = pprint.PrettyPrinter(indent=2)
ENV_VALUES = {}
CLUSTER_VALUES = {}


def cache_environments(url, params, value_dict):
    resp = requests.get(url=url, auth=base_ccloud.BASIC_AUTH, params=params)
    if resp.status_code == 200:
        out_json = resp.json()
        for item in out_json["data"]:
            value_dict[item["id"]] = item
        if 'next' in out_json['metadata']:
            query_params = parse.parse_qs(
                parse.urlsplit(out_json['metadata']['next']).query)
            params['page_token'] = str(query_params['page_token'][0])
            cache_clusters(url=url, params=params, value_dict=value_dict)
    else:
        raise Exception("Could not connect to Confluent Cloud. Please check your settings. "
                        + resp.text)


def print_env_table(val_dict):
    for k, v in val_dict.items():
        print("{:<15} {:<40}".format(
            v["id"], v["display_name"]))


def cache_clusters(url, params, value_dict):
    resp = requests.get(url=url, auth=base_ccloud.BASIC_AUTH, params=params)
    if resp.status_code == 200:
        out_json = resp.json()
        for item in out_json["data"]:
            value_dict[item["id"]] = item
        if 'next' in out_json['metadata']:
            query_params = parse.parse_qs(
                parse.urlsplit(out_json['metadata']['next']).query)
            params['page_token'] = str(query_params['page_token'][0])
            cache_clusters(url=url, params=params, value_dict=value_dict)
    else:
        raise Exception("Could not connect to Confluent Cloud. Please check your settings. "
                        + resp.text)


def print_cluster_table(val_dict):
    for v in val_dict.values():
        print("{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
            v["spec"]["environment"]["id"], v["id"], v["spec"]["display_name"], v["spec"]["cloud"], v["spec"]["availability"], v["spec"]["kafka_bootstrap_endpoint"]))


def run_cluster_workflow():
    cache_environments(
        base_ccloud.CCLOUD_URL + base_ccloud.URI_LIST['environment'], {"page_size": 20}, value_dict=ENV_VALUES)
    print("Found " + str(len(ENV_VALUES)) + " environments.")
    # print("=" * 80)
    # print_env_table(ENV_VALUES)
    # print("=" * 80)

    for env in ENV_VALUES:
        cache_clusters(base_ccloud.CCLOUD_URL + base_ccloud.URI_LIST['clusters'],
                       {"environment": env, "page_size": 20}, value_dict=CLUSTER_VALUES)
    # print("\n\n")
    print("Found " + str(len(CLUSTER_VALUES)) + " clusters.")
    # print("=" * 80)
    # print_cluster_table(CLUSTER_VALUES)
    # print("=" * 80)


if __name__ == '__main__':
    base_ccloud.initial_setup(None)
    run_cluster_workflow()
