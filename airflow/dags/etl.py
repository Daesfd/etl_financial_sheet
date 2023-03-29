import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from steps.extracao import *
from steps.transformacao import *
from steps.validacao import *

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
    """
    This function describes a DAG, which will extract income statement data, using bs4,
    load the data in a tier 1 file in a GCS bucket, then it cleans the data,
    with correct data types, loads, again, the data into a GCS bucket, but inside a tier 2 file.
    Then, it removes the local files, which won't be used in the other DAGs.

    :param dag: This function's DAG.
    :param headers: Headers used for bs4.
    :param ticker: Ticker of the chosen stock.
    :param gcs_dre_data_path_template_task_1: Path where the income statement data will be stored in a GCS' bucket.
    :param gcs_dre_data_path_template_task_2: Path where the transformed income statement data will be stored in a GCS' bucket.
    :param local_csv_dre_data_path_template: Local file path where the income statement data will be stored.
    :param local_parquet_dre_data_transf_path_template: Local file path where the transformed income statement data will be stored.
    """
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
            python_callable=data_cleaning,
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
    """
    This function describes a DAG, which extracts balance sheet data, using bs4,
    loads the data in a tier 1 file in a GCS bucket. Then, it cleans the data,
    correcting the data types, loads, again, the data into a GCS bucket, but inside a tier 2 file.
    Then, it removes the local files, which won't be used in the other DAGs.

    :param dag: This function's DAG.
    :param headers: Headers used for bs4.
    :param ticker: Ticker of the chosen stock.
    :param gcs_bp_data_path_template_task_1: Path where the balance sheet data will be stored in a GCS' bucket.
    :param gcs_bp_data_path_template_task_2: Path where the transformed balance sheet data will be stored in a GCS' bucket.
    :param local_csv_bp_data_path_template: Local file path where the balance sheet data will be stored.
    :param local_parquet_bp_data_transf_path_template: Local file path where the transformed balance sheet data will be stored.
    """
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
            python_callable=data_cleaning,
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

##############################################################################################################

#

MGLU_CSV_DRE_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/unclean_data/mglu_dre_data.csv'
MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/clean_data/mglu_dre_data_transf.parquet'
MGLU_DRE_GCS_PATH_TEMPLATE_1 = 'tier1/DRE/mglu_dre_data_raw.csv'
MGLU_DRE_GCS_PATH_TEMPLATE_2 = 'tier2/DRE/mglu_dre_data_transf.parquet'

MGLU_CSV_BP_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/unclean_data/mglu_bp_data.csv'
MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/clean_data/mglu_bp_data_transf.parquet'
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
###############################################################################################################

#

FRG_CSV_DRE_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/unclean_data/frg_dre_data.csv'
FRG_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/clean_data/frg_dre_data_transf.parquet'
FRG_DRE_GCS_PATH_TEMPLATE_1 = 'tier1/DRE/frg_dre_data_raw.csv'
FRG_DRE_GCS_PATH_TEMPLATE_2 = 'tier2/DRE/frg_dre_data_transf.parquet'

FRG_CSV_BP_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/unclean_data/frg_bp_data.csv'
FRG_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/clean_data/frg_bp_data_transf.parquet'
FRG_BP_GCS_PATH_TEMPLATE_1 = 'tier1/BP/frg_bp_data_raw.csv'
FRG_BP_GCS_PATH_TEMPLATE_2 = 'tier2/BP/frg_bp_data_transf.parquet'

frg_data_dag = DAG(
    dag_id='frg_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['frg_data']
)

dre_etl_dag(
    dag=frg_data_dag,
    headers=headers,
    ticker='FRG',
    gcs_dre_data_path_template_task_1=FRG_DRE_GCS_PATH_TEMPLATE_1,
    gcs_dre_data_path_template_task_2=FRG_DRE_GCS_PATH_TEMPLATE_2,
    local_csv_dre_data_path_template=FRG_CSV_DRE_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=FRG_PARQUET_DRE_TRANSF_FILE_TEMPLATE
)

bp_etl_dag(
    dag=frg_data_dag,
    headers=headers,
    ticker='FRG',
    gcs_bp_data_path_template_task_1=FRG_BP_GCS_PATH_TEMPLATE_1,
    gcs_bp_data_path_template_task_2=FRG_BP_GCS_PATH_TEMPLATE_2,
    local_csv_bp_data_path_template=FRG_CSV_BP_FILE_TEMPLATE,
    local_parquet_bp_data_transf_path_template=FRG_PARQUET_BP_TRANSF_FILE_TEMPLATE
)
##################################################################################################################
HNORY_CSV_DRE_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/unclean_data/hnory_dre_data.csv'
HNORY_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/clean_data/hnory_dre_data_transf.parquet'
HNORY_DRE_GCS_PATH_TEMPLATE_1 = 'tier1/DRE/hnory_dre_data_raw.csv'
HNORY_DRE_GCS_PATH_TEMPLATE_2 = 'tier2/DRE/hnory_dre_data_transf.parquet'

HNORY_CSV_BP_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/unclean_data/hnory_bp_data.csv'
HNORY_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/clean_data/hnory_bp_data_transf.parquet'
HNORY_BP_GCS_PATH_TEMPLATE_1 = 'tier1/BP/hnory_bp_data_raw.csv'
HNORY_BP_GCS_PATH_TEMPLATE_2 = 'tier2/BP/hnory_bp_data_transf.parquet'

hnory_data_dag = DAG(
    dag_id='hnory_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['hnory_data']
)

dre_etl_dag(
    dag=hnory_data_dag,
    headers=headers,
    ticker='hnory',
    gcs_dre_data_path_template_task_1=HNORY_DRE_GCS_PATH_TEMPLATE_1,
    gcs_dre_data_path_template_task_2=HNORY_DRE_GCS_PATH_TEMPLATE_2,
    local_csv_dre_data_path_template=HNORY_CSV_DRE_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=HNORY_PARQUET_DRE_TRANSF_FILE_TEMPLATE
)

bp_etl_dag(
    dag=hnory_data_dag,
    headers=headers,
    ticker='hnory',
    gcs_bp_data_path_template_task_1=HNORY_BP_GCS_PATH_TEMPLATE_1,
    gcs_bp_data_path_template_task_2=HNORY_BP_GCS_PATH_TEMPLATE_2,
    local_csv_bp_data_path_template=HNORY_CSV_BP_FILE_TEMPLATE,
    local_parquet_bp_data_transf_path_template=HNORY_PARQUET_BP_TRANSF_FILE_TEMPLATE
)
##############################################################################################################################
LRENY_CSV_DRE_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/unclean_data/lreny_dre_data.csv'
LRENY_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/clean_data/lreny_dre_data_transf.parquet'
LRENY_DRE_GCS_PATH_TEMPLATE_1 = 'tier1/DRE/lreny_dre_data_raw.csv'
LRENY_DRE_GCS_PATH_TEMPLATE_2 = 'tier2/DRE/lreny_dre_data_transf.parquet'

LRENY_CSV_BP_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/unclean_data/lreny_bp_data.csv'
LRENY_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/clean_data/lreny_bp_data_transf.parquet'
LRENY_BP_GCS_PATH_TEMPLATE_1 = 'tier1/BP/lreny_bp_data_raw.csv'
LRENY_BP_GCS_PATH_TEMPLATE_2 = 'tier2/BP/lreny_bp_data_transf.parquet'

lreny_data_dag = DAG(
    dag_id='lreny_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['lreny_data']
)

dre_etl_dag(
    dag=lreny_data_dag,
    headers=headers,
    ticker='lreny',
    gcs_dre_data_path_template_task_1=LRENY_DRE_GCS_PATH_TEMPLATE_1,
    gcs_dre_data_path_template_task_2=LRENY_DRE_GCS_PATH_TEMPLATE_2,
    local_csv_dre_data_path_template=LRENY_CSV_DRE_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=LRENY_PARQUET_DRE_TRANSF_FILE_TEMPLATE
)

bp_etl_dag(
    dag=lreny_data_dag,
    headers=headers,
    ticker='lreny',
    gcs_bp_data_path_template_task_1=LRENY_BP_GCS_PATH_TEMPLATE_1,
    gcs_bp_data_path_template_task_2=LRENY_BP_GCS_PATH_TEMPLATE_2,
    local_csv_bp_data_path_template=LRENY_CSV_BP_FILE_TEMPLATE,
    local_parquet_bp_data_transf_path_template=LRENY_PARQUET_BP_TRANSF_FILE_TEMPLATE
)
##################################################################################################################
NXGPY_CSV_DRE_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/unclean_data/nxgpy_dre_data.csv'
NXGPY_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/dre_data/clean_data/nxgpy_dre_data_transf.parquet'
NXGPY_DRE_GCS_PATH_TEMPLATE_1 = 'tier1/DRE/nxgpy_dre_data_raw.csv'
NXGPY_DRE_GCS_PATH_TEMPLATE_2 = 'tier2/DRE/nxgpy_dre_data_transf.parquet'

NXGPY_CSV_BP_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/unclean_data/nxgpy_bp_data.csv'
NXGPY_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/bp_data/clean_data/nxgpy_bp_data_transf.parquet'
NXGPY_BP_GCS_PATH_TEMPLATE_1 = 'tier1/BP/nxgpy_bp_data_raw.csv'
NXGPY_BP_GCS_PATH_TEMPLATE_2 = 'tier2/BP/nxgpy_bp_data_transf.parquet'

nxgpy_data_dag = DAG(
    dag_id='nxgpy_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['nxgpy_data']
)

dre_etl_dag(
    dag=nxgpy_data_dag,
    headers=headers,
    ticker='nxgpy',
    gcs_dre_data_path_template_task_1=NXGPY_DRE_GCS_PATH_TEMPLATE_1,
    gcs_dre_data_path_template_task_2=NXGPY_DRE_GCS_PATH_TEMPLATE_2,
    local_csv_dre_data_path_template=NXGPY_CSV_DRE_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=NXGPY_PARQUET_DRE_TRANSF_FILE_TEMPLATE
)

bp_etl_dag(
    dag=nxgpy_data_dag,
    headers=headers,
    ticker='nxgpy',
    gcs_bp_data_path_template_task_1=NXGPY_BP_GCS_PATH_TEMPLATE_1,
    gcs_bp_data_path_template_task_2=NXGPY_BP_GCS_PATH_TEMPLATE_2,
    local_csv_bp_data_path_template=NXGPY_CSV_BP_FILE_TEMPLATE,
    local_parquet_bp_data_transf_path_template=NXGPY_PARQUET_BP_TRANSF_FILE_TEMPLATE
)
