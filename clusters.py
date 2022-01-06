import pprint
from json import dumps, loads
from urllib import parse

import requests
from requests.auth import HTTPBasicAuth

import base

pp = pprint.PrettyPrinter(indent=2)


def cache_environments(url, params, value_dict):
    resp = requests.get(url=url, auth=base.BASIC_AUTH, params=params)
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
    resp = requests.get(url=url, auth=base.BASIC_AUTH, params=params)
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
    for k, v in val_dict.items():
        print("{:<15} {:<15} {:<25} {:<10} {:<25} {:<50}".format(
            v["spec"]["environment"]["id"], v["id"], v["spec"]["display_name"], v["spec"]["cloud"], v["spec"]["availability"], v["spec"]["kafka_bootstrap_endpoint"]))


def run_cluster_workflow():
    base.initial_setup()

    env_values = {}
    cache_environments(
        base.CCLOUD_URL + base.URI_LIST['environment'], {"page_size": 20}, value_dict=env_values)
    print("Found " + str(len(env_values)) + " environments.")
    print("=" * 80)
    print_env_table(env_values)
    print("=" * 80)

    cluster_values = {}
    for env in env_values:
        cache_clusters(base.CCLOUD_URL + base.URI_LIST['clusters'],
                       {"environment": env, "page_size": 20}, value_dict=cluster_values)
    print("\n\n")
    print("Found " + str(len(cluster_values)) + " clusters.")
    print("=" * 80)
    print_cluster_table(cluster_values)
    print("=" * 80)


if __name__ == '__main__':
    run_cluster_workflow()
