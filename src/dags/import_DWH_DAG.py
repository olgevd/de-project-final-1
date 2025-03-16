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
    
def load_h_currencies(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO STV2024111152__DWH.h_currencies (
	                    hk_currency_id,
                        currency_code,
                        load_dt,
                        load_src)
                    SELECT 
                        distinct hash(currency_code) as hk_currency_id,
                        currency_code,
                        now() as load_dt,
                        's3' as load_src
                    FROM STV2024111152__STAGING.сurrencies
                    WHERE hash(currency_code) not in (select hk_currency_id from STV2024111152__DWH.h_currencies)
                    """)
        
def load_h_transactions(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO STV2024111152__DWH.h_transactions (
	                    hk_operation_id,
                        operation_id,
                        load_dt,
                        load_src)
                    SELECT 
                        distinct hash(operation_id) as hk_operation_id,
                        operation_id,
                        now() as load_dt,
                        's3' as load_src
                    FROM STV2024111152__STAGING.transactions
                    WHERE hash(operation_id) not in (select hk_operation_id from STV2024111152__DWH.h_transactions)
                    """)
        
def load_l_transactions_currencies(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO STV2024111152__DWH.l_transactions_currencies (
                        hk_transactions_currencies,
                        hk_operation_id,
                        hk_currency_id,
                        load_dt,
                        load_src)
                    SELECT 
                        distinct hash(ht.hk_operation_id, hc.hk_currency_id), 
                        ht.hk_operation_id,
                        hc.hk_currency_id,
                        now() as load_dt,
                        's3' as load_src
                    FROM STV2024111152__STAGING.transactions as t
                    left join STV2024111152__DWH.h_transactions as ht on t.operation_id = ht.operation_id
                    left join STV2024111152__DWH.h_currencies as hc on hc.currency_code = t.currency_code
                    """)

def load_s_currencies(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO STV2024111152__DWH.s_currencies (
                        hk_currency_id,
                        date_update,
                        currency_code_with,
                        currency_with_div,
                        load_dt,
                        load_src)
                    SELECT 
                        hc.hk_currency_id,
                        date_update,
                        currency_code_with,
                        currency_with_div,
                        now() as load_dt,
                        's3' as load_src
                    FROM STV2024111152__STAGING.сurrencies as c
                    left join STV2024111152__DWH.h_currencies as hc on c.currency_code = hc.currency_code
                    """)
        
def load_s_transactions(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO STV2024111152__DWH.s_transactions (
                        hk_operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt,
                        load_dt,
                        load_src)
                    SELECT 
                        hk_operation_id,
                        account_number_from,
                        account_number_to,
                        currency_code,
                        country,
                        status,
                        transaction_type,
                        amount,
                        transaction_dt,
                        now() as load_dt,
                        's3' as load_src
                    FROM STV2024111152__STAGING.transactions as t
                    left join STV2024111152__DWH.h_transactions as ht on t.operation_id = ht.operation_id
                    """)

@dag(schedule_interval=('0 0 * * *'), start_date=pendulum.parse('2022-07-13'))

def final_project_dag_upload_DWH():
    task1 = PythonOperator(
        task_id=f'load_h_currencies',
        python_callable=load_h_currencies,
        op_kwargs={'key': 'data'},
    )
    task2 = PythonOperator(
        task_id=f'load_h_transactions',
        python_callable=load_h_transactions,
        op_kwargs={'key': 'data'},
    )
    task3 = PythonOperator(
        task_id=f'load_l_transactions_currencies',
        python_callable=load_l_transactions_currencies,
        op_kwargs={'key': 'data'},
    )
    task4 = PythonOperator(
        task_id=f'load_s_currencies',
        python_callable=load_s_currencies,
        op_kwargs={'key': 'data'},
    )
    task5 = PythonOperator(
        task_id=f'load_s_transactions',
        python_callable=load_s_transactions,
        op_kwargs={'key': 'data'},
    )

    task1 >> task2 >> task3 >> task4 >> task5

dwh_dag = final_project_dag_upload_DWH()


