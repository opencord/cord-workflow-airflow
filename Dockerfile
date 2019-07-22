# opencord/cord-workflow-airflow
# To build use: docker build -t opencord/cord-workflow-airflow .
# To run use: docker run -p 8080:8080 -d opencord/cord-workflow-airflow
FROM puckel/docker-airflow:1.10.2

USER root

# install dependencies of our plugin
RUN set -ex \
    && pip install multistructlog~=2.1.0 \
    && pip install cord-workflow-controller-client~=0.2.0 \
    && pip install pyfiglet~=0.7 \
    && pip install xossynchronizer~=3.2.6 \
    && pip install xosapi~=3.2.6 \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# drop plugin to plugin dir of airflow
COPY src/cord_workflow_airflow_extensions/cord_workflow_plugin.py /usr/local/airflow/plugins/

USER airflow

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

