from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd


dag =  DAG('data_processing', start_date=datetime(2024,5,10), schedule_interval="@daily", catchup=False)
    
create_table_sql = """
CREATE TABLE IF NOT EXISTS postings ( 
    id SERIAL PRIMARY KEY, 
    job_id BIGINT NOT NULL UNIQUE, 
    company_name VARCHAR, 
    title VARCHAR,
    description VARCHAR, 
    max_salary FLOAT, 
    pay_period VARCHAR,
    location VARCHAR, 
    company_id FLOAT, 
    views FLOAT, 
    med_salary FLOAT,
    min_salary FLOAT, 
    formatted_work_type VARCHAR, 
    applies FLOAT, 
    original_listed_time TIMESTAMP, 
    remote_allowed VARCHAR, 
    job_posting_url VARCHAR, 
    application_url VARCHAR, 
    application_type VARCHAR, 
    expiry TIMESTAMP,
    closed_time TIMESTAMP,
    formatted_experience_level VARCHAR, 
    skills_desc VARCHAR, 
    listed_time TIMESTAMP,
    posting_domain VARCHAR, 
    sponsored BOOLEAN,
    work_type VARCHAR,
    currency VARCHAR, 
    compensation_type VARCHAR,
    normalized_salary FLOAT, 
    zip_code FLOAT,
    fips FLOAT
);
"""
    
    
# Use PostgresOperator to create the table

create_table = PostgresOperator(
    task_id = "create_table_task",
    postgres_conn_id = 'postgress_connection',
    sql= create_table_sql,
    dag = dag 
)