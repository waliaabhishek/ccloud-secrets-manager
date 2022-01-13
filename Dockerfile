from python:3.10.1

WORKDIR /usr/src/app

COPY *.py ./
COPY requirements.txt ./
RUN pip install --no-cache-dir -r ./requirements.txt
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install
RUN export PATH=/usr/local/bin:$PATH

ENTRYPOINT [ "python", "main.py" ]
