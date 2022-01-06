# Confluent Cloud Servie Account and API Key creation Automation Package

Confluent Cloud uses Service accounts(SA) and API Keys(Keys) to interact with the Confluent Cloud Kafka Clusters.
These SA's and Keys could be generated using the CCoud API and alows for a greater integration from a CI/CD workflow. 

The long term aim of this integration is as follows:
* Allow creation of Service Accounts using a single hook from the CICD flows
* Allow creation of API Keys for the corresponding SA automatically. 
* Allow storing the SA & API Key details into a pluggable Secrets manager for ease of management and greater flexibility.
