# Copyright 2019-present Open Networking Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

ARG PYTHON_BASE_IMAGE="python:3.6-slim"

FROM ${PYTHON_BASE_IMAGE}

# Label image
ARG org_label_schema_schema_version=1.0
ARG org_label_schema_name=cord-workflow-airflow
ARG org_label_schema_version=unknown
ARG org_label_schema_vcs_url=unknown
ARG org_label_schema_vcs_ref=unknown
ARG org_label_schema_build_date=unknown
ARG org_opencord_vcs_commit_date=unknown

LABEL org.label-schema.schema-version=$org_label_schema_schema_version \
      org.label-schema.name=$org_label_schema_name \
      org.label-schema.version=$org_label_schema_version \
      org.label-schema.vcs-url=$org_label_schema_vcs_url \
      org.label-schema.vcs-ref=$org_label_schema_vcs_ref \
      org.label-schema.build-date=$org_label_schema_build_date \
      org.opencord.vcs-commit-date=$org_opencord_vcs_commit_date


SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Make sure noninteractie debian install is used and language variables set
ENV DEBIAN_FRONTEND=noninteractive LANGUAGE=C.UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8 \
    LC_CTYPE=C.UTF-8 LC_MESSAGES=C.UTF-8 TERM=linux

# Airflow
ARG AIRFLOW_VERSION=1.10.2
ARG AIRFLOW_USER=airflow
ENV AIRFLOW_USER=${AIRFLOW_USER}

ARG HOME=/home/airflow
ENV HOME=${HOME}

ARG AIRFLOW_HOME=${HOME}/airflow
ENV AIRFLOW_HOME=${AIRFLOW_HOME}


RUN apt-get update -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
        curl \
        apt-utils \
        build-essential \
        curl \
        freetds-bin \
        freetds-dev \
        libffi-dev \
        libkrb5-dev \
        libpq-dev \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        libssl-dev \
        locales  \
        netcat \
        rsync \
        sasl2-bin \
        sudo \
        default-libmysqlclient-dev \
        krb5-user \
        net-tools \
        tmux \
        unzip \
        vim \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SLUGIFY_USES_TEXT_UNIDECODE=yes
RUN pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[postgres]==${AIRFLOW_VERSION}

RUN useradd -m ${AIRFLOW_USER} \
    && echo "${AIRFLOW_USER} ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/airflow \
    && chmod 0440 /etc/sudoers.d/airflow \
    && chown -R ${AIRFLOW_USER}:${AIRFLOW_USER} ${HOME}

RUN mkdir -pv ${AIRFLOW_HOME} \
    && chown -R ${AIRFLOW_USER}:${AIRFLOW_USER} ${AIRFLOW_HOME}

COPY ./docker/script/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
COPY ./docker/config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# CORD Workflow
RUN pip install multistructlog~=2.1.0 \
    && pip install cord-workflow-controller-client~=0.3.0 \
    && pip install pyfiglet~=0.7 \
    && pip install xossynchronizer~=3.2.6 \
    && pip install xosapi~=3.2.6

# drop plugin code to plugin dir of airflow
COPY ./src/cord_workflow_airflow_extensions/cord_workflow_plugin.py ${AIRFLOW_HOME}/plugins/cord_workflow_plugin.py

# make xosapi directory
RUN mkdir -p /var/run/xosapi \
    && chmod a+rwx /var/run/xosapi

# drop sample workflow code to dags dir of airflow
COPY ./workflow_examples/sequential-cord-workflow/sequential_cord_workflow.py ${AIRFLOW_HOME}/dags/sequential_cord_workflow.py
COPY ./workflow_examples/sequential-cord-workflow/sequential_cord_workflow_essence.json ${HOME}/sequential_cord_workflow_essence.json
COPY ./workflow_examples/parallel-cord-workflow/parallel_cord_workflow.py ${AIRFLOW_HOME}/dags/parallel_cord_workflow.py
COPY ./workflow_examples/parallel-cord-workflow/parallel_cord_workflow_essence.json ${HOME}/parallel_cord_workflow_essence.json
COPY ./workflow_examples/att-workflow/att_workflow.py ${AIRFLOW_HOME}/dags/att_workflow.py
COPY ./workflow_examples/att-workflow/att_workflow_essence.json ${HOME}/att_workflow_essence.json
COPY ./workflow_examples/simple-airflow-workflow/simple_airflow_workflow.py ${AIRFLOW_HOME}/dags/simple_airflow_workflow.py
COPY ./workflow_examples/simple-airflow-workflow/simple_airflow_workflow_essence.json ${HOME}/simple_airflow_workflow_essence.json

# copy scripts
COPY ./workflow_examples/scripts ${HOME}/scripts

# copy kickstarter code & workflow ctl code
COPY ./src/tools/kickstarter.py ${HOME}/kickstarter.py
COPY ./src/tools/workflow_ctl.py ${HOME}/workflow_ctl.py

# copy configuration json
COPY ./src/tools/config.json /etc/cord_workflow_airflow_extensions/config.json

RUN chown -R ${AIRFLOW_USER}:${AIRFLOW_USER} ${HOME} \
    && chmod 755 -R ${HOME}/scripts \
    && chmod 755 ${HOME}/kickstarter.py \
    && chmod 755 ${HOME}/workflow_ctl.py

# 8080 for webserver (admin_ui)
# 8793 for worker log
EXPOSE 8080 8793

USER ${AIRFLOW_USER}
WORKDIR ${HOME}

ENTRYPOINT ["/home/airflow/airflow/entrypoint.sh"]
