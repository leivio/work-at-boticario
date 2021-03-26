import tweepy as tw
import os
from pathlib import Path
import csv
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime


consumer_key = "ekEVFsTqzWpKggpk151ayjdDZ"
consumer_secret = "FFJIeHfNEDOwKq2aYoo3rbNGySreeEweRaYiXSZ7TjkYvlnAtO"


def datatwitter():
    auth = tw.AppAuthHandler(consumer_key, consumer_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    search_words = 'boticario AND perfumaria'
    new_search = search_words + " -filter:retweets"

    tweets = tw.Cursor(api.search,
                       q=new_search,
                       lang="pt",
                       since="2021-03-01",
                       tweet_mode='extended').items(50)
    return tweets


def get_conn_sql():
    server = "34.70.9.36"
    database = "postgres"
    username = "sqldb01"
    password = "!@#123qwe123"
    sslmode = "require"
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(server, username, database,
                                                                                 password, sslmode)
    cnn = psycopg2.connect(conn_string)
    print("Conexao OK")
    return cnn


def query_create():
    query = """ 
                    DO $$
                    BEGIN
                      if exists(select * from information_schema.tables where table_name = 'twitters') then  
                        truncate table twitters;  
                      else 
                        create table twitters (users varchar(80), locale varchar(80), texts varchar(500));
                      end if;
                    END; 
                    $$ 
                """
    return query


def query_insert():
    query = """                 
                 insert into twitters (users, locale, texts) values(%s,%s,%s)
                """
    return query



def create_table():
    sql = get_conn_sql()
    cur = sql.cursor()
    cur.execute(query_create())
    sql.commit()
    cur.close()
    sql.close()


def twitters_to_sql():
    sql = get_conn_sql()
    cur = sql.cursor()
    tweets = datatwitter()

    for tweet in tweets:
        if not tweet.retweeted:
            arg = (tweet.user.screen_name, tweet.user.location, tweet.full_text)
            cur.execute(query_insert(), arg)
            sql.commit()

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
    dag_id='dag_twitter',
    default_args=args,
    dagrun_timeout=timedelta(minutes=1)
)


run_create_table = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)


run_twitters_to_sql = PythonOperator(
    task_id='twitters_to_sql',
    python_callable=twitters_to_sql,
    dag=dag,
)


run_create_table >> run_twitters_to_sql
