from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
import numpy as np
from datetime import date

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text, inspect


def loading_branch_service():
    df_branch_service = pd.read_json("/opt/airflow/data/branch_service_transaction_info.json")
    df_branch_service.to_parquet("/opt/airflow/data/df_1_branch_service.parquet")


def loading_customer_transaction():
    df_customer_transaction = pd.read_json("/opt/airflow/data/customer_transaction_info.json")
    df_customer_transaction.to_parquet("/opt/airflow/data/df_2_customer_transaction.parquet")


def drop_dups_branch_service():
     df_branch_service = pd.read_parquet('/opt/airflow/data/df_1_branch_service.parquet', engine='pyarrow')
     df_branch_service = df_branch_service.drop_duplicates(subset=['txn_id'])
     df_branch_service.to_parquet("/opt/airflow/data/df_3_branch_service_drop_dups.parquet")


def drop_dups_customer_transaction():
     df_customer_transaction = pd.read_parquet('/opt/airflow/data/df_2_customer_transaction.parquet', engine='pyarrow')
     df_customer_transaction = df_customer_transaction.drop_duplicates(subset=['txn_id'])
     df_customer_transaction.to_parquet("/opt/airflow/data/df_4_customer_transaction_drop_dups.parquet")


def merge_dataframe():
     df_branch_service = pd.read_parquet('/opt/airflow/data/df_3_branch_service_drop_dups.parquet', engine='pyarrow')
     df_customer_transaction = pd.read_parquet('/opt/airflow/data/df_4_customer_transaction_drop_dups.parquet', engine='pyarrow')
     df_merged = pd.merge(df_customer_transaction, df_branch_service)
     df_merged.to_parquet('/opt/airflow/data/df_5_no_dups_merged.parquet')


def fill_branch_name():
    df_merged = pd.read_parquet('/opt/airflow/data/df_5_no_dups_merged.parquet')
    df_merged['branch_name'] = df_merged.replace('',np.nan).groupby('txn_id')['branch_name'].transform('first')
    df_merged['branch_name'] = df_merged['branch_name'].ffill().bfill()
    df_merged.to_parquet('/opt/airflow/data/df_6_branch_name_filled.parquet')
    
    
def fill_price():
    df_merged = pd.read_parquet('/opt/airflow/data/df_6_branch_name_filled.parquet')
    df_merged['price'] = df_merged['price'].fillna(df_merged.groupby(['branch_name','service'])['price'].transform('mean'))
    df_merged.to_parquet('/opt/airflow/data/df_7_price_filled.parquet')
    
    
def standardize_last_name():
    df_merged = pd.read_parquet('/opt/airflow/data/df_7_price_filled.parquet')
    df_merged['last_name'] = df_merged['last_name'].str.replace('\W', '', regex=True)
    df_merged['last_name'] = df_merged['last_name'].str.upper()
    df_merged.to_parquet('/opt/airflow/data/df_8_standardized_last_name.parquet')
    
    
def standardize_first_name():
    df_merged = pd.read_parquet('/opt/airflow/data/df_8_standardized_last_name.parquet')
    df_merged['first_name'] = df_merged['first_name'].str.replace('\W', '', regex=True)
    df_merged['first_name'] = df_merged['first_name'].str.upper()
    df_merged.to_parquet('/opt/airflow/data/df_9_standardized_first_name.parquet')


def validate_dates():
    df_merged = pd.read_parquet('/opt/airflow/data/df_9_standardized_first_name.parquet')
    today = str(date.today())
    df_merged['avail_date'] = pd.to_datetime(df_merged['avail_date'], format='%Y-%m-%d')
    df_merged['birthday'] = pd.to_datetime(df_merged['birthday'], format='%Y-%m-%d')
    df_merged = df_merged[(df_merged['avail_date'] <= today) & (df_merged['birthday'] <= today)]
    df_merged = df_merged[(df_merged['avail_date'] > df_merged['birthday'])]
    df_merged.to_parquet('/opt/airflow/data/df_10_validated_dates.parquet')
    
    
def validate_price():
    df_merged = pd.read_parquet('/opt/airflow/data/df_10_validated_dates.parquet')
    df_merged['price'] = df_merged['price'].round(2)
    df_merged.to_parquet('/opt/airflow/data/df_11_validated_price.parquet')
        
def ingestion_to_db():
    df_merged = pd.read_parquet('/opt/airflow/data/df_11_validated_price.parquet')
        
    
    dbname = "postgres"
    user ="postgres"
    password ="kyle1018"
    host = "localhost"
    port = "5432"

    flag = False

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    conn.autocommit = True

    cur = conn.cursor()

    cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = {}").format(sql.Literal("Transaction")))
    exists = cur.fetchone()

    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("Transaction")))

    conn.commit()
        
    engine = create_engine('postgresql://postgres:%s@localhost:5432/Transaction' % (password))

    inspector = inspect(engine)
    table_exists = inspector.has_table('txn')

    if not table_exists:
        with engine.connect() as connection:
            connection.execute(text("DROP VIEW IF EXISTS weekly_report CASCADE"))

        df_merged.to_sql('txn', con=engine, if_exists='replace', index=False)
        print("Table 'txn' created.")
    else:
        print("Table 'txn' already exists. Skipping creation.")

    dbname2 = "Transaction"

    conn2 = psycopg2.connect(dbname=dbname2, user=user, password=password, host=host, port=port)
    conn2.autocommit = True

    cur2 = conn2.cursor()

    cur2.execute("""
    CREATE OR REPLACE VIEW weekly_report AS 
    SELECT
        EXTRACT(YEAR FROM avail_date) AS year,
        EXTRACT(WEEK FROM avail_date) AS week_of_year,
        service,
        ROUND(SUM(price)::numeric, 2) AS weekly_sales
    FROM
        txn 
    GROUP BY
        year, week_of_year, service 
    ORDER BY
        year ASC, week_of_year ASC, service ASC;
    """)


    cur2.close()
    conn2.close()
    cur.close()
    conn.close()
    
    
args = {
    'owner': 'Zrey',
    'start_date': days_ago(0),
    
}

dag = DAG(
    dag_id='labex3_dag',
    default_args=args,
    schedule_interval='@hourly',
    max_active_runs= 1
)

with dag:

        loading_branch_service = PythonOperator(
            task_id="loading_branch_service",
            python_callable=loading_branch_service
        )

        loading_customer_transaction = PythonOperator(
            task_id="loading_customer_transaction",
            python_callable=loading_customer_transaction
        )

        drop_dups_branch_service = PythonOperator(
            task_id="drop_dups_branch_service",
            python_callable=drop_dups_branch_service
        )

        drop_dups_customer_transaction = PythonOperator(
            task_id="drop_dups_customer_transaction",
            python_callable=drop_dups_customer_transaction
        )

        merge_dataframe = PythonOperator(
            task_id="merge_dataframe",
            python_callable=merge_dataframe
        )
        
        fill_branch_name = PythonOperator(
            task_id="fill_branch_name",
            python_callable=fill_branch_name
        )
        
        fill_price = PythonOperator(
            task_id="fill_price",
            python_callable=fill_price
        )
        
        standardize_last_name = PythonOperator(
            task_id="standardize_last_name",
            python_callable=standardize_last_name
        )
        
        standardize_first_name = PythonOperator(
            task_id="standardize_first_name",
            python_callable=standardize_first_name
        )
        
        validate_dates = PythonOperator(
            task_id="validate_dates",
            python_callable=validate_dates
        )
        
        validate_price = PythonOperator(
            task_id="validate_price",
            python_callable=validate_price
        )
        
        ingestion_to_db = PythonOperator(
            task_id="ingestion_to_db",
            python_callable=ingestion_to_db
        )


        
loading_branch_service >> drop_dups_branch_service
loading_customer_transaction >> drop_dups_customer_transaction
[drop_dups_branch_service, drop_dups_customer_transaction] >> merge_dataframe
merge_dataframe >> fill_branch_name >> fill_price >> standardize_last_name
standardize_last_name >> standardize_first_name >> validate_dates >> validate_price >> ingestion_to_db