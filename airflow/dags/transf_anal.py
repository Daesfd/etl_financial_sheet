import os

import pendulum
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


def ratios_acquisition_dag(
        dag,
        local_parquet_bp_data_transf_path_template,
        local_parquet_dre_data_transf_path_template,
        output_file_path,
        gcs_inc_data_path_template_task_3
):
    """
    This function describes a DAG, which, from locally data files,
    that were got from bp_etl_dag and dre_etl_data, gets financials ratios.
    Then, it loads the data in a tier 2 GCS bucket.
    Lastly, it removes the local files, which won't be used in the other DAGs.

    :param dag: This function's DAG.
    :param local_parquet_bp_data_transf_path_template: Local path file where the financial sheet data is stored.
    :param local_parquet_dre_data_transf_path_template: Local path file where the income statement data is stored.
    :param output_file_path: Local path file where the ratio data will be stored.
    :param gcs_inc_data_path_template_task_3: Path where the ratio data will be stored in a GCS' bucket.
    """
    with dag:

        get_ratios_task = PythonOperator(
            task_id='get_ratios_task',
            python_callable=get_ratios,
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
            bash_command=f"rm {local_parquet_bp_data_transf_path_template} {local_parquet_dre_data_transf_path_template}"
        )

        get_ratios_task >> local_to_gcs_task3_tier_2 >> rm_task


def acquisition_avg_sector_values_dag(
        dag,
        path_to_lren_ind,
        path_to_hnory_ind,
        path_to_frg_ind,
        path_to_nxgpy_ind,
        avg_path_file,
        gcs_inc_data_path_template_task_4

):
    """
    This function describe a DAG, which, from locally data files,
    that were got from ratios_acquisition_dag, gets the average financial ratios,
    from the stocks below.
    Then, it loads the data in a tier 3 GCS bucket.
    Lastly, it removes the local files, which won't be used in the other DAGs.

    :param dag: This function's DAG
    :param path_to_lren_ind: Local path file where the ratio data from lren is stored.
    :param path_to_hnory_ind: Local path file where the ratio data from hnory is stored.
    :param path_to_frg_ind: Local path file where the ratio data from frg is stored.
    :param path_to_nxgpy_ind: Local path file where the ratio data from nxgpy is stored.
    :param avg_path_file: Local path file where the average ratio data from will be stored
    :param gcs_inc_data_path_template_task_4: Path where the average ratio data will be stored in GCS' bucket.
    :return:
    """
    with dag:

        avg_sector_values_task = PythonOperator(
            task_id='avg_sector_values_dag',
            python_callable=avg_sector_values,
            op_kwargs={
                'path_to_lren_ind': path_to_lren_ind,
                'path_to_hnory_ind': path_to_hnory_ind,
                'path_to_frg_ind': path_to_frg_ind,
                'path_to_nxgpy_ind': path_to_nxgpy_ind,
                'output_path_file': avg_path_file
            }
        )

        local_to_gcs_task4_tier_3 = PythonOperator(
            task_id='local_to_gcs_task4',
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_inc_data_path_template_task_4,
                "local_file": avg_path_file,
            }
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {avg_path_file} {path_to_lren_ind} {path_to_hnory_ind} {path_to_frg_ind} {path_to_nxgpy_ind}"
        )

        avg_sector_values_task >> local_to_gcs_task4_tier_3 >> rm_task


#

FRG_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/frg_ratio.parquet'
HNORY_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/hnory_ratio.parquet'
LRENY_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/lreny_ratio.parquet'
NXGPY_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/nxgpy_ratio.parquet'
AVERAGE_RATIO_FILE_TEMPLATE = AIRFLOW_HOME + '/avg_ratio.parquet'
AVERAGE_RATIO_GCS_PATH_TEMPLATE = 'tier3/RATIO/avg_ratio.parquet'


avg_sector_values_dag = DAG(
    dag_id='avg_sector_values_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['avg_sector_values_data']
)

acquisition_avg_sector_values_dag(
    dag=avg_sector_values_dag,
    path_to_lren_ind=LRENY_RATIO_FILE_TEMPLETE,
    path_to_hnory_ind=HNORY_RATIO_FILE_TEMPLETE,
    path_to_frg_ind=FRG_RATIO_FILE_TEMPLETE,
    path_to_nxgpy_ind=NXGPY_RATIO_FILE_TEMPLETE,
    avg_path_file=AVERAGE_RATIO_FILE_TEMPLATE,
    gcs_inc_data_path_template_task_4=AVERAGE_RATIO_GCS_PATH_TEMPLATE
)


######################################################################################################################

#

MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_dre_data_transf.parquet'
MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/mglu_bp_data_transf.parquet'
MGLU_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/mglu_ratio.parquet'
MGLU_RATIO_GCS_PATH_TEMPLATE = 'tier2/RATIO/mglu_ratio.parquet'

mglu_ratio_data_dag = DAG(
    dag_id='mglu_ratio_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['mglu_ratio_data']
)

ratios_acquisition_dag(
    dag=mglu_ratio_data_dag,
    local_parquet_bp_data_transf_path_template=MGLU_PARQUET_BP_TRANSF_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=MGLU_PARQUET_DRE_TRANSF_FILE_TEMPLATE,
    output_file_path=MGLU_RATIO_FILE_TEMPLETE,
    gcs_inc_data_path_template_task_3=MGLU_RATIO_GCS_PATH_TEMPLATE
)
####################################################################################################################
FRG_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/frg_dre_data_transf.parquet'
FRG_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/frg_bp_data_transf.parquet'
FRG_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/frg_ratio.parquet'
FRG_RATIO_GCS_PATH_TEMPLATE = 'tier2/RATIO/frg_ratio.parquet'

frg_ratio_data_dag = DAG(
    dag_id='frg_ratio_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['frg_ratio_data']
)

ratios_acquisition_dag(
    dag=frg_ratio_data_dag,
    local_parquet_bp_data_transf_path_template=FRG_PARQUET_BP_TRANSF_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=FRG_PARQUET_DRE_TRANSF_FILE_TEMPLATE,
    output_file_path=FRG_RATIO_FILE_TEMPLETE,
    gcs_inc_data_path_template_task_3=FRG_RATIO_GCS_PATH_TEMPLATE
)
###########################################################################################################
HNORY_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/hnory_dre_data_transf.parquet'
HNORY_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/hnory_bp_data_transf.parquet'
HNORY_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/hnory_ratio.parquet'
HNORY_RATIO_GCS_PATH_TEMPLATE = 'tier2/RATIO/hnory_ratio.parquet'

hnory_ratio_data_dag = DAG(
    dag_id='hnory_ratio_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['hnory_ratio_data']
)

ratios_acquisition_dag(
    dag=hnory_ratio_data_dag,
    local_parquet_bp_data_transf_path_template=HNORY_PARQUET_BP_TRANSF_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=HNORY_PARQUET_DRE_TRANSF_FILE_TEMPLATE,
    output_file_path=HNORY_RATIO_FILE_TEMPLETE,
    gcs_inc_data_path_template_task_3=HNORY_RATIO_GCS_PATH_TEMPLATE
)
############################################################################################################
LRENY_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/lreny_dre_data_transf.parquet'
LRENY_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/lreny_bp_data_transf.parquet'
LRENY_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/lreny_ratio.parquet'
LRENY_RATIO_GCS_PATH_TEMPLATE = 'tier2/RATIO/lreny_ratio.parquet'

lreny_ratio_data_dag = DAG(
    dag_id='lreny_ratio_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['lreny_ratio_data']
)

ratios_acquisition_dag(
    dag=lreny_ratio_data_dag,
    local_parquet_bp_data_transf_path_template=LRENY_PARQUET_BP_TRANSF_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=LRENY_PARQUET_DRE_TRANSF_FILE_TEMPLATE,
    output_file_path=LRENY_RATIO_FILE_TEMPLETE,
    gcs_inc_data_path_template_task_3=LRENY_RATIO_GCS_PATH_TEMPLATE
)
########################################################################################################
NXGPY_PARQUET_DRE_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/nxgpy_dre_data_transf.parquet'
NXGPY_PARQUET_BP_TRANSF_FILE_TEMPLATE = AIRFLOW_HOME + '/nxgpy_bp_data_transf.parquet'
NXGPY_RATIO_FILE_TEMPLETE = AIRFLOW_HOME + '/nxgpy_ratio.parquet'
NXGPY_RATIO_GCS_PATH_TEMPLATE = 'tier2/RATIO/nxgpy_ratio.parquet'

nxgpy_ratio_data_dag = DAG(
    dag_id='nxgpy_ratio_data',
    schedule_interval='@once',
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=0),
    max_active_runs=1,
    catchup=True,
    tags=['nxgpy_ratio_data']
)

ratios_acquisition_dag(
    dag=nxgpy_ratio_data_dag,
    local_parquet_bp_data_transf_path_template=NXGPY_PARQUET_BP_TRANSF_FILE_TEMPLATE,
    local_parquet_dre_data_transf_path_template=NXGPY_PARQUET_DRE_TRANSF_FILE_TEMPLATE,
    output_file_path=NXGPY_RATIO_FILE_TEMPLETE,
    gcs_inc_data_path_template_task_3=NXGPY_RATIO_GCS_PATH_TEMPLATE
)
