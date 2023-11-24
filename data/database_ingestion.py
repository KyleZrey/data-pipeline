
import pandas as pd

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, text, inspect

def main():
    df_merged = pd.read_parquet('C:/airflow-docker/data/df_11_validated_price.parquet')
        
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