import vertica_python
import pendulum

import botocore
import boto3

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag


conn_info = {'host': 'vertica.tgcloudenv.ru', 
             'port': '5433',
             'user': 'stv2024111152',       
             'password': 'hcsA3Km0hjNQ47q',
             'database': 'dwh',
             # Вначале автокоммит понадобится, а позже решите сами.
                         'autocommit': True
}


def get_curr_id(key: str):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""select id
                        from STV2024111152__STAGING.transactions_log
                        where file_name = '{key}'
                        order by id desc
                        limit 1""")  
        result = cur.fetchall()
        if result:
            print(f"Next one id is {int(result[0][0]) + 1}")
            return int(result[0][0]) + 1
        else:
            print(f"Increment for {key} does not exists, returning id 1")
            return 1
            

def update_id(id: int, key: str):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"""insert into STV2024111152__STAGING.transactions_log (id, file_name)
                    VALUES ({id}, '{key}')""") 


def fetch_s3_file(bucket: str, key: str):
    i = get_curr_id(key)
    file_name = key.replace('.csv', f'{i}.csv')

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net/',
        config=botocore.client.Config(signature_version=botocore.UNSIGNED))

    print('/data/' + file_name)

    s3_client.download_file(
        Bucket=bucket,
        Key=file_name,
        Filename='/data/' + file_name)    
    
    return i

    
def load_transactions(key: str, conn_info=conn_info, **kwargs):
    ti = kwargs['ti']
    i = ti.xcom_pull(task_ids='load_transactions_csv')
    file_name = key.replace('.csv', f'{i}.csv')
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        # cur.execute("""delete from STV2024111152__STAGING.transactions;""")            
        cur.execute(f"""
                    COPY STV2024111152__STAGING.transactions (
                        operation_id, 
                        account_number_from, 
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt
                )
                    FROM LOCAL '/data/{file_name}'
                    DELIMITER ','
                    ENCLOSED BY '"' """)
    update_id(i, key)

@dag(schedule_interval=('0 0 * * *'), start_date=pendulum.parse('2022-07-13'))

def final_project_dag_upload_transactions():
    task1 = PythonOperator(
        task_id=f'load_transactions_csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'final-project', 'key': 'transactions_batch_.csv'},
    ),
    task3 = PythonOperator(
        task_id=f'load_transactions',
        python_callable=load_transactions,
        op_kwargs={'key': 'transactions_batch_.csv'},
    )

    task1 >> task3

s3_dag = final_project_dag_upload_transactions()