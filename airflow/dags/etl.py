import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from steps.extracao import *
from steps.transformacao import *

from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}



def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    # "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def dre_etl_dag(
        dag,
        headers,
        ticker,
        gcs_path_template_task_1,
        gcs_path_template_task_2,
        local_csv_path_template,
        local_parquet_transf_path_template
):

    with dag:
        extract_dre_data_task = PythonOperator(
            task_id='extract_dre_data_task',
            python_callable=get_dre_data,
            op_kwargs={
                'headers': headers,
                'ticker': ticker,
                'output_file': local_csv_path_template
            }
        )

        local_to_gcs_task1 = PythonOperator(
            task_id="local_to_gcs_task1",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template_task_1,
                "local_file": local_csv_path_template,
            },
        )

        transf_dre_data_task = PythonOperator(
            task_id='transf_dre_data_task',
            python_callable=limpeza_dos_dados,
            op_kwargs={
                'input_file': local_csv_path_template,
                'output_file': local_parquet_transf_path_template
            },
        )

        local_to_gcs_task2 = PythonOperator(
            task_id="local_to_gcs_task2",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template_task_2,
                "local_file": local_parquet_transf_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_transf_path_template}"
        )

        extract_dre_data_task >> local_to_gcs_task1 >> transf_dre_data_task >> local_to_gcs_task2 >> rm_task


MGLU_CSV_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_dre_data.csv'
MGLU_CSV_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_dre_data_raw.csv'
MGLU_PARQUET_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_dre_data_transf.parquet'
MGLU_GCS_PATH_TEMPLATE_1 = 'tier1/mglu/mglu_dre_data_raw.csv'
MGLU_GCS_PATH_TEMPLATE_2 = 'tier2/mglu/mglu_dre_data_transf.parquet'


mglu_dre_data_dag = DAG(
    dag_id='mglu_dre_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=1),
    max_active_runs=1,
    catchup=True,
    tags=['dre_data']
)

dre_etl_dag(
    dag=mglu_dre_data_dag,
    headers=headers,
    ticker='mgluy',
    gcs_path_template_task_1=MGLU_GCS_PATH_TEMPLATE_1,
    gcs_path_template_task_2=MGLU_GCS_PATH_TEMPLATE_2,
    local_csv_path_template=MGLU_CSV_FILE_TEMPLATE,
    local_parquet_transf_path_template=MGLU_PARQUET_TRANSF_FILE_TEMPLATE
)
