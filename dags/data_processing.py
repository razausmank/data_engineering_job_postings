from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine,  Column, Integer, String, DateTime, Float, Boolean, BigInteger, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import  declarative_base
import os 
import pandas as pd

def _process_csv(): 
    """reads csv's into chunks, transforms the chunks
    and pushes it to the database"""
    engine = setup_database_connection()
    chunk_size = 10000

    for  chunk in pd.read_csv('data/postings.csv', chunksize=chunk_size): 
        print(chunk) 
        
        chunk['original_listed_time'] = pd.to_datetime(chunk['original_listed_time'],unit='ms')
        chunk['expiry'] = pd.to_datetime(chunk['expiry'],unit='ms')
        chunk['closed_time'] = pd.to_datetime(chunk['closed_time'],unit='ms')
        chunk['listed_time'] = pd.to_datetime(chunk['listed_time'],unit='ms')
        chunk['sponsored'] = chunk['sponsored'].astype(bool)
        chunk['remote_allowed'] = chunk['remote_allowed'].astype(float)
        
        chunk.to_sql('temp_table', engine, if_exists= 'replace', index=False)
        column_names_for_Sql_query = '''
        job_id, company_name, title, description, max_salary, pay_period, location, company_id, views, med_salary, min_salary, formatted_work_type, applies, original_listed_time, remote_allowed, job_posting_url, application_url, application_type, expiry,closed_time, formatted_experience_level, skills_desc, listed_time, posting_domain, sponsored, work_type, currency,  compensation_type, normalized_salary, zip_code, fips 
        '''
        engine.execute(text(f"""
        INSERT INTO postings ( {column_names_for_Sql_query})
        SELECT {column_names_for_Sql_query} FROM temp_table
        ON CONFLICT (job_id) DO NOTHING;
        """))


def setup_database_connection():

    # Retrieve PostgreSQL connection parameters from environment variables
    username = os.getenv('POSTGRES_DB_USERNAME')
    password = os.environ.get('POSTGRES_DB_PASSWORD')
    database = os.environ.get('POSTGRES_DB_NAME')
    host = os.environ.get('POSTGRES_DB_HOST', 'localhost')  # Default to 'localhost' if not set
    port = os.environ.get('POSTGRES_DB_PORT', '5432')        # Default to '5432' if not set

    print('username')

    print(username)
    # Check if required environment variables are set
    if not username or not password or not database:
        raise ValueError("Database connection parameters are not set in the environment variables.")

    # Create a connection string
    connection_string = f'postgresql://{username}:{password}@{host}:{port}/{database}'

    print(connection_string)
    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Check the database connection
    try:
        with engine.connect() as connection:
            print("Database connection successful!")
    except SQLAlchemyError as e:
        print(f"Database connection failed: {e}")
        return None  # Return None if the connection fails

    return engine  # Return the engine if the connection is successful

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





process_csv = PythonOperator(
    task_id = 'process_csv', 
    python_callable = _process_csv,
    dag = dag 
)



