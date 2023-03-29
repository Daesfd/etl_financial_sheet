FROM apache/airflow:2.3.4-python3.9

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim -qqq
# git gcc g++ -qqq
RUN mkdir "/opt/airflow/bp_data" "/opt/airflow/dre_data" "/opt/airflow/ratio" \
    "/opt/airflow/bp_data/clean_data" "/opt/airflow/bp_data/unclean_data" "/opt/airflow/bp_data/suites" \
    "/opt/airflow/dre_data/clean_data" "/opt/airflow/dre_data/unclean_data" "/opt/airflow/dre_data/suites" \
    "/opt/airflow/ratio/suites" "/opt/airflow/validations"


RUN chmod -R 777 "/opt/airflow/bp_data" "/opt/airflow/dre_data" "/opt/airflow/ratio" \
    "/opt/airflow/bp_data/clean_data" "/opt/airflow/bp_data/unclean_data" "/opt/airflow/bp_data/suites" \
    "/opt/airflow/dre_data/clean_data" "/opt/airflow/dre_data/unclean_data" "/opt/airflow/dre_data/suites" \
    "/opt/airflow/ratio/suites" "/opt/airflow/validations"

USER airflow

COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER root

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/home/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts

USER $AIRFLOW_UID