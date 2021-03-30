from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta, datetime


args = {
    'owner': 'leivio',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 28),
    'email': ['leivio@yahoo.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@hourly',
}


dag = DAG(
    dag_id='dag_fill_tables',
    default_args=args
)

# a. Consolidado de vendas por ano e mês
run_has_vendas_ano_mes = PostgresOperator(
        task_id='has_vendas_ano_mes',
        postgres_conn_id='dbpostgres',
        sql='sql/has_table.sql',
        params={'tables': 'tb_vendas_ano_mes'},
        dag=dag
    )


run_tb_vendas_ano_mes = PostgresOperator(
        task_id='tb_vendas_ano_mes',
        postgres_conn_id='dbpostgres',
        sql='sql/tb_vendas_ano_mes.sql',
        dag=dag
    )

# b. Consolidado de vendas por marca e linha
run_has_vendas_marca_linha = PostgresOperator(
        task_id='has_vendas_marca_linha',
        postgres_conn_id='dbpostgres',
        sql='sql/has_table.sql',
        params={'tables': 'tb_vendas_marca_linha'},
        dag=dag
    )


run_tb_vendas_marca_linha = PostgresOperator(
        task_id='tb_vendas_marca_linha',
        postgres_conn_id='dbpostgres',
        sql='sql/tb_vendas_marca_linha.sql',
        dag=dag
    )

# c. Consolidado de vendas por marca, ano e mês;
run_has_vendas_marca_ano_mes = PostgresOperator(
        task_id='has_vendas_marca_ano_mes',
        postgres_conn_id='dbpostgres',
        sql='sql/has_table.sql',
        params={'tables': 'tb_vendas_marca_ano_mes'},
        dag=dag
    )


run_tb_vendas_marca_ano_mes = PostgresOperator(
        task_id='tb_vendas_marca_ano_mes',
        postgres_conn_id='dbpostgres',
        sql='sql/tb_vendas_marca_ano_mes.sql',
        dag=dag
    )

# d. Consolidado de vendas por linha, ano e mês
run_has_vendas_linha_ano_mes = PostgresOperator(
        task_id='has_vendas_linha_ano_mes',
        postgres_conn_id='dbpostgres',
        sql='sql/has_table.sql',
        params={'tables': 'tb_vendas_linha_ano_mes'},
        dag=dag
    )


run_tb_vendas_linha_ano_mes = PostgresOperator(
        task_id='tb_vendas_linha_ano_mes',
        postgres_conn_id='dbpostgres',
        sql='sql/tb_vendas_linha_ano_mes.sql',
        dag=dag
    )

run_has_vendas_ano_mes >> run_tb_vendas_ano_mes
run_has_vendas_marca_linha >> run_tb_vendas_marca_linha
run_has_vendas_marca_ano_mes >> run_tb_vendas_marca_ano_mes
run_has_vendas_linha_ano_mes >> run_tb_vendas_linha_ano_mes