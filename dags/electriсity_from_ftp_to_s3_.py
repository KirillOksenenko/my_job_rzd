from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def upload_to_s3(**context):
    """Загрузка данных в S3 (MinIO) и создание мета-информации"""
    logger = logging.getLogger(__name__)

    logger.info("Загружаем данные в S3...")

    # Создаем hook для S3 (MinIO)
    s3_hook = S3Hook(aws_conn_id='minio_conn_electricity')

    # Загружаем файл напрямую
    local_file_path = '/opt/airflow/dags/data_set/coteq_electricity_2020.csv'
    bucket_name = 'electricity'
    file_name = f'energy_consumption_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

    # Читаем данные для мета-информации
    df = pd.read_csv(local_file_path)

    # Сохраняем мета-информацию для отчета
    meta_info = {
        'total_records': len(df),
        'columns_count': len(df.columns),
        's3_path': f's3://{bucket_name}/{file_name}'
    }
    context['ti'].xcom_push(key='upload_meta', value=meta_info)

    # Загружаем файл в S3
    s3_hook.load_file(
        filename=local_file_path,
        key=file_name,
        bucket_name=bucket_name,
    )

    logger.info(f"Данные загружены в S3: s3://{bucket_name}/{file_name}")
    logger.info(f"Загружено {len(df)} записей")


def create_summary_report(**context):
    """Создание отчета о загрузке"""
    logger = logging.getLogger(__name__)

    # Получаем мета-информацию из задачи загрузки
    upload_meta = context['ti'].xcom_pull(key='upload_meta', task_ids='upload_to_s3')

    logger.info("Создаем отчет о загрузке...")

    report = {
        'total_records': upload_meta['total_records'],
        'columns_count': upload_meta['columns_count'],
        's3_location': upload_meta['s3_path'],
        'load_timestamp': datetime.now().isoformat()
    }

    logger.info(f"Отчет: {report}")

    return context['ti'].xcom_push(key='final_report', value=report)


with DAG(
        'electriсity_from_ftp_to_s3_data_loader',
        default_args=default_args,
        description='DAG для загрузки данных в S3',
        schedule_interval='0 * * * *',
        catchup=False,
        tags=['energy', 's3']
) as dag:
    start = DummyOperator(task_id='start')

    upload_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True
    )

    create_report_task = PythonOperator(
        task_id='create_report',
        python_callable=create_summary_report,
        provide_context=True
    )

    end = DummyOperator(task_id='end')

    start >> upload_s3_task >> create_report_task >> end