import vertica_python
import pendulum

import botocore
import boto3

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag


def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net/',
        config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename='/data/'+key)    


conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2024111152',       
             'password': 'hcsA3Km0hjNQ47q',
             'database': 'dwh',
             # Вначале автокоммит понадобится, а позже решите сами.
                         'autocommit': True
}
    
def load_curriencies(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        # cur.execute("""delete from STV2024111152__STAGING.сurrencies;""")
        cur.execute("""
                    COPY STV2024111152__STAGING.сurrencies (
	                    currency_code,
	                    currency_code_with,
                        date_update,
	                    currency_with_div
                )
                    FROM LOCAL '/data/currencies_history.csv'
                    DELIMITER ','
                    ENCLOSED BY '"' """)


@dag(schedule_interval=('0 0 * * *'), start_date=pendulum.parse('2022-07-13'))

def final_project_dag_upload_сurrencies():
    task1 = PythonOperator(
        task_id=f'load_сurrencies_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'final-project', 'key': 'currencies_history.csv'},
    ),
    task3 = PythonOperator(
        task_id=f'load_сurrencies',
        python_callable=load_curriencies,
        op_kwargs={'key': 'currencies_history.csv'},
    )

s3_dag = final_project_dag_upload_сurrencies()