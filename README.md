# Confluent Cloud Service Account and API Key Management Automation Package

Service accounts(SA) and API Keys(Keys) are used to interact with Confluent Cloud Kafka Clusters. These Kafka clusters exist in logical Environments. The SA are created at a Organization level and the Keys are linked to an SA and Kafka Cluster. 

Currently, there are CLI and API available to generate and work with SA & API Keys but no direct path with CI/CD flows integration. This project aims to solve that. The core definitions of all the Service Accounts and necessary API Key bindings will be maintained in a single definitions file. This definitions file is a YAML structured file that will be used to maintain all definitions in a single location.

The project will use that definitions file to set up the Service accounts as well as to generate the API Keys. After generating the API Key/Secret pair; it will also append the API Key/Secret combo to a Secret Management layer of your choice all while adding tags for quick management and search capability. 

The long term aim of this integration is as follows:
* Allow creation of Service Accounts using standardized YAML structure - Done
* Allow creation of API Keys for the corresponding SA automatically - Done
* Allow storing the SA & API Key details into a pluggable Secrets manager for ease of management and greater flexibility - Done for AWS Secrets Manager with more to follow.
* Enable Permission management for the Pluggable Secret Manger, so that it can become one stop shop for Confluent Cloud secret management - Not started yet.


## Quickstart

The execution starts with the base python file `main_yaml_runner.py`. This file has the following switches available: 

* `--csm-config-file-path`: This is the configuration file path that will provide the connectivity and other config details. Sample file is available inside the configurations folder with the name `config.yaml`
* `--csm-definitions-file-path`: This is the definition file path that will provide the resource definitions for execution in CCloud. Sample file is available inside the configurations folder with the name `definitions.yaml`
* `--csm-generate-definitions-file`: This switch can be used for the initial runs where the team does not have a definitions file and would like to auto generate one from the existing ccloud resource mappings. 
* `--dry-run`: This switch can be used to invoke a dry run and list all the action that will be preformed, but not performing them.
* `--disable-api-key-creation`: This switch can be used to disable API Key & Secret creation (if required)
* `--print-delete-eligible-api-keys`: This switch can be used to print the API keys which are not synced to the Secret store and (potentially) not used.

There is more documentation available what switches are available within each file inside the `configurations` folder.

