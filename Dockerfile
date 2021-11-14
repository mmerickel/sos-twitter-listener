FROM python:3.9

ENV DOCKERIZE_VERSION v0.6.1

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y wget \
    && wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && apt-get clean

RUN set -e \
    && mkdir /app && cd /app \
    && python -m pip install pipenv

WORKDIR /app

COPY Pipfile Pipfile.lock ./

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y build-essential \
    && pipenv install \
    && apt-get remove -y build-essential \
    && apt-get clean

COPY sos/ ./sos
COPY profile.yml.tmpl ./profile.yml.tmpl

ENTRYPOINT ["dockerize", "-template", "profile.yml.tmpl:profile.yml", "pipenv", "run"]
