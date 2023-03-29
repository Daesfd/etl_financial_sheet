import os

import pendulum
from airflow import DAG
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


def dre_data_validation_dag(
        dag,
        local_parquet_dre_data_transf_path_template,
        suite_dre_folder,
        data_folder,
        expectation_suite_file_path,
        validated_data_folder,
):

    with dag:

        get_dre_expectations_suite_task = PythonOperator(
            task_id='get_dre_expectations_suite_task',
            python_callable=get_dre_data_expectations,
            op_kwargs={
                'input_file': local_parquet_dre_data_transf_path_template,
                'output_file_folder': suite_dre_folder,
            }
        )

        file_validation_task = PythonOperator(
            task_id='file_validation_task',
            python_callable=validate_all_files,
            op_kwargs={
                'folder_path': data_folder,
                'expectation_suite': expectation_suite_file_path,
                'output_path': validated_data_folder,
            }
        )

        get_dre_expectations_suite_task >> file_validation_task


def bal_pat_data_validation_dag(
        dag,
        local_parquet_bp_data_transf_path_template,
        validation_bp_folder,
        data_folder,
        expectation_suite_file_path,
        validated_data_folder,
):

    with dag:

        get_bp_expectations_suite_task = PythonOperator(
            task_id='get_bp_expectations_suite_task',
            python_callable=get_bal_pat_data_expectations,
            op_kwargs={
                'input_file': local_parquet_bp_data_transf_path_template,
                'output_file_folder': validation_bp_folder,
            }
        )

        file_validation_task = PythonOperator(
            task_id='file_validation_task',
            python_callable=validate_all_files,
            op_kwargs={
                'folder_path': data_folder,
                'expectation_suite': expectation_suite_file_path,
                'output_path': validated_data_folder,

            }
        )

        get_bp_expectations_suite_task >> file_validation_task


def ratio_data_validation_dag(
        dag,
        local_parquet_ratio_data_transf_path_template,
        validation_ratio_folder,
        data_folder,
        expectation_suite_file_path,
        validated_data_folder,
):

    with dag:

        get_ratio_expectations_suite_task = PythonOperator(
            task_id='get_ratio_expectations_suite_task',
            python_callable=get_ratio_data_expectations,
            op_kwargs={
                'input_file': local_parquet_ratio_data_transf_path_template,
                'output_file_folder': validation_ratio_folder,
            }
        )

        file_validation_task = PythonOperator(
            task_id='file_validation_task',
            python_callable=validate_all_files,
            op_kwargs={
                'folder_path': data_folder,
                'expectation_suite': expectation_suite_file_path,
                'output_path': validated_data_folder,
            }
        )

        get_ratio_expectations_suite_task >> file_validation_task



MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/clean_data/mglu_dre_data_transf.parquet'
DRE_SUITE_FOLDER = AIRFLOW_HOME + '/dre_data/suites'
DRE_DATA_FOLDER = AIRFLOW_HOME + '/dre_data/clean_data'
EXPECTATION_DRE_SUITE_FILE_PATH = AIRFLOW_HOME + '/dre_data/suites/dre_suite.json'
VALIDATIONS_FOLDER = AIRFLOW_HOME + '/validations'


dre_val_dag = DAG(
    dag_id='dre_validation_dag',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['dre_validation_dag']
)

dre_data_validation_dag(
    dag=dre_val_dag,
    local_parquet_dre_data_transf_path_template=MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE,
    suite_dre_folder=DRE_SUITE_FOLDER,
    data_folder=DRE_DATA_FOLDER,
    expectation_suite_file_path=EXPECTATION_DRE_SUITE_FILE_PATH,
    validated_data_folder=VALIDATIONS_FOLDER,
)

######################################################################################################################

MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/clean_data/mglu_bp_data_transf.parquet'
BP_SUITE_FOLDER = AIRFLOW_HOME + '/bp_data/suites'
BP_DATA_FOLDER = AIRFLOW_HOME + '/bp_data/clean_data'
EXPECTATION_BP_SUITE_FILE_PATH = AIRFLOW_HOME + '/bp_data/suites/bp_suite.json'
VALIDATIONS_FOLDER = AIRFLOW_HOME + '/validations'

bp_val_dag = DAG(
    dag_id='bp_validation_dag',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['bp_validation_dag']
)

bal_pat_data_validation_dag(
    dag=bp_val_dag,
    local_parquet_bp_data_transf_path_template=MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE,
    validation_bp_folder=BP_SUITE_FOLDER,
    data_folder=BP_DATA_FOLDER,
    expectation_suite_file_path=EXPECTATION_BP_SUITE_FILE_PATH,
    validated_data_folder=VALIDATIONS_FOLDER,
)

######################################################################################################################

MGLU_PARQUET_RATIO_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/ratio/mglu_ratio.parquet'
RATIO_SUITE_FOLDER = AIRFLOW_HOME + '/ratio/suites'
RATIO_DATA_FOLDER = AIRFLOW_HOME + '/ratio'
EXPECTATION_RATIO_SUITE_FILE_PATH = AIRFLOW_HOME + '/ratio/suites/ratio_suite.json'
VALIDATIONS_FOLDER = AIRFLOW_HOME + '/validations'

ratio_val_dag = DAG(
    dag_id='ratio_validation_dag',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['ratio_validation_dag']
)

ratio_data_validation_dag(
    dag=ratio_val_dag,
    local_parquet_ratio_data_transf_path_template=MGLU_PARQUET_RATIO_TRANSF_FILE_TEMPLATE,
    validation_ratio_folder=RATIO_SUITE_FOLDER,
    data_folder=RATIO_DATA_FOLDER,
    expectation_suite_file_path=EXPECTATION_RATIO_SUITE_FILE_PATH,
    validated_data_folder=VALIDATIONS_FOLDER,
)
