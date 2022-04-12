from python:3.10.1

WORKDIR /usr/src/app

# Copy code to the image
COPY *.py ./
COPY app_managers/*.py app_managers/
COPY app_managers/core/*.py app_managers/core/
COPY app_managers/workflow_manager/*.py app_managers/workflow_manager/
COPY secret_managers/*.py secret_managers/
COPY ccloud_managers/*.py ccloud_managers/
COPY requirements.txt ./
# Installl the requirements
RUN pip install --no-cache-dir -r ./requirements.txt
# Install Confluent CLI
RUN curl -sL --http1.1 https://cnfl.io/cli | sh -s -- -b /usr/local/bin latest
# Add both to the path for easy access
RUN export PATH=/usr/local/bin:$PATH

ENTRYPOINT [ "python", "main_cicd_runner.py" ]
CMD [ "--csm-config-file-path", "configurations/config.yaml", "--csm-definitions-file-path", "configurations/definitions.yaml", "--print-delete-eligible-api-keys", "--dry-run"]