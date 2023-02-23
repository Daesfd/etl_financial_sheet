import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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


def aquisicao_de_indicadores_dag(
        dag,
        local_parquet_bp_data_transf_path_template,
        local_parquet_dre_data_transf_path_template,
        output_file_path,
        gcs_inc_data_path_template_task_3
):
    with dag:

        get_indicadores = PythonOperator(
            task_id='get_indicadores',
            python_callable=get_indicadores,
            op_kwargs={
                'balanco_patrimonial_data_file': local_parquet_bp_data_transf_path_template,
                'dre_data_file': local_parquet_dre_data_transf_path_template,
                'output_file': output_file_path
            }
        )

        local_to_gcs_task3_tier_2 = PythonOperator(
            task_id='local_to_gcs_task3',
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_inc_data_path_template_task_3,
                "local_file": output_file_path,
            }
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_parquet_bp_data_transf_path_template} {local_parquet_dre_data_transf_path_template} {output_file_path}"
        )

        get_indicadores >> local_to_gcs_task3_tier_2 >> rm_task


def get_media_do_setor(
        dag,
        path_to_lren_ind,
        path_to_hnory_ind,
        path_to_frg_ind,
        path_to_nxgpy_ind,
        media_path_file,
        gcs_inc_data_path_template_task_4

):
    with dag:

        obtencao_da_media_do_setor_task = PythonOperator(
            task_id='obtencao_media_do_setor',
            python_callable=obtencao_da_media_do_setor,
            op_kwargs={
                'path_to_lren_ind': path_to_lren_ind,
                'path_to_hnory_ind': path_to_hnory_ind,
                'path_to_frg_ind': path_to_frg_ind,
                'path_to_nxgpy_ind': path_to_nxgpy_ind,
                'output_path_file': media_path_file
            }
        )

        local_to_gcs_task4_tier_3 = PythonOperator(
            task_id='local_to_gcs_task4',
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_inc_data_path_template_task_4,
                "local_file": media_path_file,
            }
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {media_path_file}"
        )

        obtencao_da_media_do_setor_task >> local_to_gcs_task4_tier_3 >> rm_task


