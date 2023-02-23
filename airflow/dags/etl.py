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
        gcs_dre_data_path_template_task_1,
        gcs_dre_data_path_template_task_2,
        local_csv_dre_data_path_template,
        local_parquet_dre_data_transf_path_template
):

    with dag:
        extract_dre_data_task = PythonOperator(
            task_id='extract_dre_data_task',
            python_callable=get_dre_data,
            op_kwargs={
                'headers': headers,
                'ticker': ticker,
                'output_file': local_csv_dre_data_path_template
            }
        )

        local_to_gcs_task1_tier_1 = PythonOperator(
            task_id="local_to_gcs_dre_data_task1",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_dre_data_path_template_task_1,
                "local_file": local_csv_dre_data_path_template,
            },
        )

        transf_dre_data_task = PythonOperator(
            task_id='transf_dre_data_task',
            python_callable=limpeza_dos_dados,
            op_kwargs={
                'input_file': local_csv_dre_data_path_template,
                'output_file': local_parquet_dre_data_transf_path_template
            },
        )

        local_to_gcs_task2_tier_2 = PythonOperator(
            task_id="local_to_gcs_dre_data_task2",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_dre_data_path_template_task_2,
                "local_file": local_parquet_dre_data_transf_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_dre_data_task",
            bash_command=f"rm {local_csv_dre_data_path_template}"
        )

        extract_dre_data_task >> local_to_gcs_task1_tier_1 >> transf_dre_data_task >> local_to_gcs_task2_tier_2 >> rm_task


def bp_etl_dag(
        dag,
        headers,
        ticker,
        gcs_bp_data_path_template_task_1,
        gcs_bp_data_path_template_task_2,
        local_csv_bp_data_path_template,
        local_parquet_bp_data_transf_path_template
):

    with dag:

        extract_bp_data_task = PythonOperator(
            task_id='extract_bp_data_task',
            python_callable=get_bal_pat_data,
            op_kwargs={
                'headers': headers,
                'ticker': ticker,
                'output_file': local_csv_bp_data_path_template
            }
        )

        local_to_gcs_task1_tier_1 = PythonOperator(
            task_id="local_to_gcs_bp_data_task1",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_bp_data_path_template_task_1,
                "local_file": local_csv_bp_data_path_template,
            },
        )

        transf_bp_data_task = PythonOperator(
            task_id='transf_bp_data_task',
            python_callable=limpeza_dos_dados,
            op_kwargs={
                'input_file': local_csv_bp_data_path_template,
                'output_file': local_parquet_bp_data_transf_path_template
            },
        )

        local_to_gcs_task2_tier_2 = PythonOperator(
            task_id="local_to_gcs_bp_data_task2",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_bp_data_path_template_task_2,
                "local_file": local_parquet_bp_data_transf_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_bp_data_task",
            bash_command=f"rm {local_csv_bp_data_path_template}"
        )

        extract_bp_data_task >> local_to_gcs_task1_tier_1 >> transf_bp_data_task >> local_to_gcs_task2_tier_2 >> rm_task


MGLU_CSV_DRE_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_dre_data.csv'
MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_dre_data_transf.parquet'
MGLU_DRE_GCS_PATH_TEMPLATE_1 = 'tier1/DRE/mglu_dre_data_raw.csv'
MGLU_DRE_GCS_PATH_TEMPLATE_2 = 'tier2/DRE/mglu_dre_data_transf.parquet'

MGLU_CSV_BP_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_bp_data.csv'
MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_bp_data_transf.parquet'
MGLU_BP_GCS_PATH_TEMPLATE_1 = 'tier1/BP/mglu_bp_data_raw.csv'
MGLU_BP_GCS_PATH_TEMPLATE_2 = 'tier2/BP/mglu_bp_data_transf.parquet'

mglu_data_dag = DAG(
    dag_id='mglu_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['mglu_data']
)

dre_etl_dag(
    dag=mglu_data_dag,
    headers=headers,
    ticker='mgluy',
    gcs_dre_data_path_template_task_1=MGLU_DRE_GCS_PATH_TEMPLATE_1,
    gcs_dre_data_path_template_task_2=MGLU_DRE_GCS_PATH_TEMPLATE_2,
    local_csv_dre_data_path_template=MGLU_CSV_DRE_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE
)

bp_etl_dag(
    dag=mglu_data_dag,
    headers=headers,
    ticker='mgluy',
    gcs_bp_data_path_template_task_1=MGLU_BP_GCS_PATH_TEMPLATE_1,
    gcs_bp_data_path_template_task_2=MGLU_BP_GCS_PATH_TEMPLATE_2,
    local_csv_bp_data_path_template=MGLU_CSV_BP_FILE_TEMPLATE,
    local_parquet_bp_data_transf_path_template=MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE
)
