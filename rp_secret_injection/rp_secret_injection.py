import subprocess
import argparse
from json import dumps, loads
from base64 import b64decode, b64encode
import sys
from typing import Dict, List
import re


def printline():
    print("=" * 80)


def execute_subcommand(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    out = process.communicate()[0].strip()
    return out.decode("UTF-8")


def get_k8s_secret(k8s_ns: str, secret_name: str):
    cmd_to_execute = f"kubectl -n {k8s_ns} get secret {secret_name} -o json"
    resp = loads(execute_subcommand(cmd_to_execute))
    return resp


def patch_k8s_secret(k8s_ns: str, secret_name: str, secret_key_id: str, new_value: str):
    json_data = dumps({"op": "replace", "path": f"/data/{secret_key_id}", "value": new_value})
    cmd_to_execute = f"kubectl -n {k8s_ns} patch secret {secret_name} --type='json' -p='[{json_data}]'"
    resp = execute_subcommand(cmd_to_execute)
    return resp


def get_basic_secret(k8s_ns: str, secret_name: str, secret_key_id: str):
    resp = get_k8s_secret(k8s_ns, secret_name)
    output = resp["data"].get(secret_key_id, None)
    if output:
        output = str(b64decode(output), "utf-8")
        output = [v for v in output.splitlines() if v]
    return output


def get_jaas_secret(k8s_ns: str, secret_name: str, secret_key_id: str):
    resp = get_k8s_secret(k8s_ns, secret_name)
    output = resp["data"].get(secret_key_id, None)
    if output:
        output = str(b64decode(output), "utf-8")
        prepend, kclient, data = output.partition("KafkaClient")
        data = data.split("org.apache.kafka.common.security.plain.PlainLoginModule required")[1:]
        return_data = []
        for item in data:
            pair = re.findall(r'"(.*?)"', item)
            return_data.append({"username": pair[0], "password": pair[1]})
        prepend = str(prepend + kclient + " {\n")
        postpend = "};\n"
        return prepend, postpend, return_data
    else:
        return None


def update_basic_secret(k8s_ns: str, secret_name: str, secret_key_id: str, secret_data: List[str]):
    secret_data.append("")
    ascii_data = "\n".join(secret_data).encode("ascii")
    b64_data = b64encode(ascii_data).decode("ascii")
    resp = patch_k8s_secret(k8s_ns, secret_name, secret_key_id, b64_data)
    return resp


def update_jaas_secret(
    k8s_ns: str, secret_name: str, secret_key_id: str, secret_data: List[Dict[str, str]], prepend: str, postpend: str
):
    in_string = ""
    for item in secret_data:
        in_string = str(
            in_string
            + f"  org.apache.kafka.common.security.plain.PlainLoginModule required\n  username=\"{item['username']}\"\n  password=\"{item['password']}\";\n\n"
        )
    ascii_data = "".join((prepend, in_string, postpend)).encode("ascii")
    b64_data = b64encode(ascii_data).decode("ascii")
    resp = patch_k8s_secret(k8s_ns, secret_name, secret_key_id, b64_data)
    return resp


def check_presence(key_name: str, key_value: str):
    if key_value:
        return key_value
    else:
        raise Exception(f"Please ensure that '{key_name}' is provided in the config file")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Command line arguments for controlling the application",
        add_help=True,
    )

    global_args = parser.add_mutually_exclusive_group()
    conf_args = parser.add_argument_group("switch-args", "Switch based argument run")
    file_args = parser.add_argument_group("file-args", "File based argument run")

    global_args.add_argument(
        "--enable-file-config-loader",
        default=None,
        action="store_true",
        help="This loader can be used to feed the data from property files instead of using inline switches.",
    )
    global_args.add_argument(
        "--enable-switch-config-loader",
        default=None,
        action="store_true",
        help="This loader can be used to feed the data from switches instead of using file based property feeds.",
    )
    # global_args.add_argument(
    #     "--dry-run",
    #     default=False,
    #     action="store_true",
    #     help="This switch can be used to invoke a dry run and list all the action that will be preformed, but not performing them.",
    # )

    file_args.add_argument(
        "--config-file-path",
        type=str,
        default=argparse.SUPPRESS,
        required="--enable-file-config-loader" in sys.argv,
        metavar="<file path of the config props file",
        help="This is the config file that contains the property values needed for executing the script via file loader.",
    )

    conf_args.add_argument(
        "--k8s-namespace",
        type=str,
        default=argparse.SUPPRESS,
        required="--enable-switch-config-loader" in sys.argv,
        metavar="confluent-namespace-value",
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )
    conf_args.add_argument(
        "--basic-users-secret-name",
        type=str,
        default=argparse.SUPPRESS,
        required="--enable-switch-config-loader" in sys.argv,
        metavar="k8s-secret-name",
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )
    conf_args.add_argument(
        "--basic-users-key",
        type=str,
        default=argparse.SUPPRESS,
        required="--enable-switch-config-loader" in sys.argv,
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )
    conf_args.add_argument(
        "--jaas-users-secret-name",
        type=str,
        default=argparse.SUPPRESS,
        required="--enable-switch-config-loader" in sys.argv,
        metavar="k8s-secret-name",
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )
    conf_args.add_argument(
        "--rp-users-jaas-key",
        type=str,
        default=argparse.SUPPRESS,
        required="--enable-switch-config-loader" in sys.argv,
        metavar="key-name-inside-jaas-secret",
        help="This is the definition file path that will provide the resource definitions for execution in CCloud.",
    )

    args = parser.parse_args()

    # Raise parser exception if neither of the mutually exclusive switches are provided.
    if not args.enable_file_config_loader and not args.enable_switch_config_loader:
        parser.error(
            "No config loader type provided, add --enable-file-config-loader or --enable-switch-config-loader"
        )

    # Ensure all the properties are provided in the file being parsed.
    if args.enable_file_config_loader:
        with open(args.config_file_path, "r") as f:
            config_string = "[section]\n" + f.read()
        from configparser import ConfigParser

        file_parser = ConfigParser()
        file_parser.read_string(config_string)
        k8s_ns = check_presence("k8s_namespace", file_parser.get("section", "k8s_namespace"))
        k8s_secret_basic_users = check_presence(
            "basic_users_secret_name", file_parser.get("section", "basic_users_secret_name")
        )
        k8s_secret_basic_users_key = check_presence("basic_users_key", file_parser.get("section", "basic_users_key"))
        k8s_secret_jaas_users = check_presence(
            "jaas_users_secret_name", file_parser.get("section", "jaas_users_secret_name")
        )
        k8s_secret_jaas_users_key = check_presence("jaas_users_key", file_parser.get("section", "jaas_users_key"))

    if args.enable_switch_config_loader:
        k8s_ns = args.k8s_namespace
        k8s_secret_basic_users = args.basic_users_secret_name
        k8s_secret_basic_users_key = args.basic_users_key
        k8s_secret_jaas_users = args.jaas_users_secret_name
        k8s_secret_jaas_users_key = args.rp_users_jaas_key

    # Input read for API Key & Secret
    input_api_key = input("Please provide the New API Key:")
    input_api_secret = input("Please provide the New API Secret:")
    if not input_api_key or not input_api_secret:
        raise Exception(
            "The API Key or Secret value was left empty, please ensure that you provide the right value for processing."
        )
    else:
        new_api_key = input_api_key
        new_api_secret = input_api_secret

    # Confirm the Input value
    break_loop = False
    while not break_loop:
        answer = input("Is the API Key and Secret provided correct?(Y or N)").upper()
        if answer in ["YES", "Y"]:
            break_loop = True
        elif answer in ["NO", "N"]:
            print("Bye!")
            exit(1)
        else:
            print("Please use Y or N as the response. Try again.")

    printline()
    # is_dry_run = args.dry_run

    print("Current Values that the script will be working with: ")
    # print(f"Is Dry Run:\t\t\t\t\t\t\t{is_dry_run}")
    print(f"New API Key:\t\t\t\t\t\t\t{new_api_key}")
    print(f"Target Kubernetes Namespace:\t\t\t\t\t{k8s_ns}")
    print(f"Target Basic Auth Kubernetes Secret Name:\t\t\t{k8s_secret_basic_users}")
    print(f"Target Basic Auth Key to locate inside secret:\t\t\t{k8s_secret_basic_users_key}")
    print(f"Target Kafka JAAS Users Kubernetes Secret Name:\t\t\t{k8s_secret_jaas_users}")
    print(f"Target Kafka JAAS Users Key to locate inside secret:\t\t{k8s_secret_jaas_users_key}")

    was_patching_performed = False

    printline()
    # Check Basic Users and add if necessary
    basic_users = get_basic_secret(k8s_ns, k8s_secret_basic_users, k8s_secret_basic_users_key)
    # print(resp)
    no_update_required, found_update_required, update_resp = False, False, ""
    for i, item in enumerate(basic_users):
        secret, _, _ = item.partition(",krp-users")
        key, _, value = secret.partition(":")
        key, value = key.strip(), value.strip()
        if new_api_key == key and new_api_secret == value:
            no_update_required = True
            break
        if new_api_key == key and new_api_secret != value:
            basic_users[i] = f"{new_api_key}: {new_api_secret},krp-users"
            found_update_required = True
        del secret, key, value
    if no_update_required:
        print(f"Found the API Key in {k8s_secret_basic_users_key} key in {k8s_secret_basic_users} secret")
        print("Not updating anything")
    elif found_update_required:
        print("API Key located but the secret value was different. Updating secret value now.")
        update_resp = update_basic_secret(k8s_ns, k8s_secret_basic_users, k8s_secret_basic_users_key, basic_users)
        print(update_resp)
        was_patching_performed = True
    else:
        print(f"API Key not found in the secret. Updating the secret now.")
        basic_users.append(f"{new_api_key}: {new_api_secret},krp-users")
        update_resp = update_basic_secret(k8s_ns, k8s_secret_basic_users, k8s_secret_basic_users_key, basic_users)
        print(update_resp)
        was_patching_performed = True
    del no_update_required, found_update_required, update_resp

    printline()
    # Check JAAS Users and add if necessary
    prepend, postpend, jaas_users = get_jaas_secret(k8s_ns, k8s_secret_jaas_users, k8s_secret_jaas_users_key)
    no_update_required, found_update_required, update_resp = False, False, ""
    for i, item in enumerate(jaas_users):
        if item["username"] == new_api_key and item["password"] == new_api_secret:
            no_update_required = True
            break
        if item["username"] == new_api_key and item["password"] != new_api_secret:
            jaas_users[i] = {"username": new_api_key, "password": new_api_secret}
            found_update_required = True
    if no_update_required:
        print(f"Found the API Key in {k8s_secret_jaas_users_key} key in {k8s_secret_jaas_users} secret")
        print("Not updating anything")
    elif found_update_required:
        print("API Key located but the secret value was different. Updating secret value now.")
        update_resp = update_jaas_secret(
            k8s_ns, k8s_secret_jaas_users, k8s_secret_jaas_users_key, jaas_users, prepend, postpend
        )
        print(update_resp)
        was_patching_performed = True
    else:
        print(f"API Key not found in the secret. Updating the secret now.")
        jaas_users.append({"username": new_api_key, "password": new_api_secret})
        update_resp = update_jaas_secret(
            k8s_ns, k8s_secret_jaas_users, k8s_secret_jaas_users_key, jaas_users, prepend, postpend
        )
        print(update_resp)
        was_patching_performed = True

    printline()
    if was_patching_performed:
        print(
            "Secrets were updated in the Kubernetes Cluster. Please ensure that you rollout restart the Component to apply the new secret values."
        )
