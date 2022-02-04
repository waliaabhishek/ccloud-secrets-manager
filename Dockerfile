from python:3.10.1

WORKDIR /usr/src/app

# Copy code to the image
COPY *.py ./
COPY requirements.txt ./
# Installl the requirements
RUN pip install --no-cache-dir -r ./requirements.txt
# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
# Install Confluent CLI
RUN curl -sL --http1.1 https://cnfl.io/cli | sh -s -- -b /usr/local/bin latest
# Add both to the path for easy access
RUN export PATH=/usr/local/bin:$PATH

ENTRYPOINT [ "python", "main_yaml_runner.py", "--csm-config-file-path", "configurations/config.yaml", "--csm-definitions-file-path", "configurations/definitions.yaml", "--print-delete-eligible-api-keys", "--dry-run" ]
