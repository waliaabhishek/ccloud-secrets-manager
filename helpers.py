import pprint
from os import environ

ENV_PREFIX = "env::"
pretty = pprint.PrettyPrinter(indent=2)


def get_env_var(var_name: str):
    if environ.get(var_name) is None:
        raise Exception("Cannot find environment variable " + var_name)
    else:
        return environ[var_name]


def find_replace_env_vars(input: str, env_prefix=ENV_PREFIX):
    if input.startswith(env_prefix):
        input = input.split(env_prefix)[1]
        return get_env_var(input)
    else:
        return input


def env_parse_replace(input):
    if isinstance(input, dict):
        for k, v in input.items():
            if isinstance(v, dict) or isinstance(v, list):
                env_parse_replace(v)
            elif isinstance(v, str):
                input[k] = find_replace_env_vars(v)


def mandatory_check(key, value):
    if not value:
        raise Exception(key + " is a mandatory attribute. Please populate to ensure correct functionality.")


if __name__ == "__main__":
    test = ["env:safdsaf", "regular", ENV_PREFIX + "CONFLUENT_CLOUD_EMAIL"]

    for item in test:
        print(find_replace_env_vars(item))
