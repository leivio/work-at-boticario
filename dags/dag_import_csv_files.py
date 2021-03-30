import os
from pathlib import Path
import csv
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta, datetime


def get_conn_sql():
    server = "34.70.9.36"
    database = "postgres"
    username = "sqldb01"
    password = "!@#123qwe123"
    sslmode = "require"
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(server, username, database, password, sslmode)
    cnn = psycopg2.connect(conn_string)
    print("Conexao OK")
    return cnn


def query_create():
    query = """ 
                DO $$
                BEGIN
                  if exists(select * from information_schema.tables where table_name = 'sales') then  
                    truncate table sales;  
                  else 
                    create table sales (id_marca int, marca varchar(80), id_linha int, linha varchar(80),data_venda date, qtd_venda int);
                  end if;
                END; 
                $$ 
            """
    return query


def query_insert():
    query = """
             SET datestyle = dmy;
             insert into sales(id_marca, marca, id_linha, linha, data_venda, qtd_venda) values(%s,%s,%s,%s,%s,%s)
            """
    return query


# caso exista limpa a tabela se não cria
def create_table():
    sql = get_conn_sql()
    cur = sql.cursor()
    cur.execute(query_create())
    sql.commit()
    cur.close()
    sql.close()


def csv_to_sql():
    sql = get_conn_sql()
    cur = sql.cursor()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    print(dir_path)
    arr = os.listdir("/home/airflow/gcs/data")
    for s in arr:
        path = Path(__file__).parent.parent
        file = os.path.join(path, "data", s)
        with open(file, mode='r', encoding="utf8") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 0
            for row in csv_reader:
                if line_count > 0:
                    arg = (int(row[0]), row[1], int(row[2]), row[3], row[4], int(row[5]))
                    cur.execute(query_insert(), arg)
                    sql.commit()
                    # print("Insert " + row[0])
                line_count += 1
    cur.close()
    sql.close()

args = {
    'owner': 'leivio',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 23),
    'email': ['leivio@yahoo.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@hourly',
}


dag = DAG(
    dag_id='dag_import_csv',
    default_args=args
)


run_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)


run_csv_to_sql = PythonOperator(
    task_id='csv_to_sql',
    python_callable=csv_to_sql,
    dag=dag,
)



run_create_table >> run_csv_to_sql