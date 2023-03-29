import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from steps.validacao import *

from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


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


def validated_data_to_gcs_task(
        dag,
        validated_data_folder,
        gcs_dre_validation_file_path_template,
        gcs_bp_validation_file_path_template,
        gcs_ratio_validation_file_path_template,
        dre_data_folder,
        bp_data_folder,
        ratio_data_folder
):

    with dag:

        for file in os.listdir(validated_data_folder):

            if file.split('_')[2] == 'dre':

                validation_dre_file_to_gcs_task = PythonOperator(
                    task_id=f'local_to_gcs_dre_validation_task_{file.split("_")[1]}',
                    python_callable=upload_to_gcs,
                    op_kwargs={
                        "bucket": BUCKET,
                        "object_name": f'{gcs_dre_validation_file_path_template}/{file.split("_")[1]}.json',
                        "local_file": f'{validated_data_folder}/{file}',
                    }
                )

            elif file.split('_')[2] == 'bp':

                validation_bp_file_to_gcs_task = PythonOperator(
                    task_id=f'local_to_gcs_bp_validation_task_{file.split("_")[1]}',
                    python_callable=upload_to_gcs,
                    op_kwargs={
                        "bucket": BUCKET,
                        "object_name": f'{gcs_bp_validation_file_path_template}/{file.split("_")[1]}.json',
                        "local_file": f'{validated_data_folder}/{file}',
                    }
                )

            else:

                validation_ratio_file_to_gcs_task = PythonOperator(
                    task_id=f'local_to_gcs_ratio_validation_task_{file.split("_")[1]}',
                    python_callable=upload_to_gcs,
                    op_kwargs={
                        "bucket": BUCKET,
                        "object_name": f'{gcs_ratio_validation_file_path_template}/{file.split("_")[1]}.json',
                        "local_file": f'{validated_data_folder}/{file}',
                    }
                )

        rm_task = BashOperator(
            task_id='rm_task',
            bash_command=f'rm {validated_data_folder}/*.json {dre_data_folder}/*.parquet {bp_data_folder}/*.parquet {ratio_data_folder}/*.parquet'
        )

        validation_bp_file_to_gcs_task >> validation_ratio_file_to_gcs_task >> validation_dre_file_to_gcs_task >> rm_task


VALIDATED_DATA_FOLDER = AIRFLOW_HOME + '/validations'
GCS_DRE_PATH_TEMPLATE = 'tier2/validations/dre'
GCS_BP_PATH_TEMPLATE = 'tier2/validations/bp'
GCS_RATIO_PATH_TEMPLATE = 'tier2/validations/ratio'
DRE_DATA_FOLDER = AIRFLOW_HOME + '/dre_data/clean_data'
BP_DATA_FOLDER = AIRFLOW_HOME + '/bp_data/clean_data'
RATIO_DATA_FOLDER = AIRFLOW_HOME + '/ratio'


val_data_to_gcs_dag = DAG(
    dag_id='val_data_to_gcs_dag',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['val_data_to_gcs_dag']
)

validated_data_to_gcs_task(
    dag=val_data_to_gcs_dag,
    validated_data_folder=VALIDATED_DATA_FOLDER,
    gcs_dre_validation_file_path_template=GCS_DRE_PATH_TEMPLATE,
    gcs_bp_validation_file_path_template=GCS_BP_PATH_TEMPLATE,
    gcs_ratio_validation_file_path_template=GCS_RATIO_PATH_TEMPLATE,
    dre_data_folder=DRE_DATA_FOLDER,
    bp_data_folder=BP_DATA_FOLDER,
    ratio_data_folder=RATIO_DATA_FOLDER
)
