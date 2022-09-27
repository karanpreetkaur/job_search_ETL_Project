#!/usr/bin/env python

# Author: Karanpreet Kaur
# date: 2022-09-03

"""ETL Job Logging system"""

import os
from datetime import datetime
import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib.parse

# Load environment file
load_dotenv()

# Configure logging
logging.basicConfig(filename='jobs_logging.log', level=logging.DEBUG)

# Read connection details
DB_HOST = os.environ.get("DB_HOST")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PORT = os.environ.get("DB_PORT")


def create_target_database(database_name, engine):
    """ Creates target in the PostgreSQL database """
    #Preparing query to create a database
    sql = """ CREATE DATABASE target """

    #Creating a database
    with engine.begin() as conn:
        conn.autocommit = True
        result = conn.execution_options(isolation_level="AUTOCOMMIT").execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'target'")
        exists = result.fetchone()
        if not exists:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(sql)
    conn.close()

    # if not exists:
    #     try:
    #         with engine.begin() as conn:
    #             conn.execute(sql)
    #             logging.info(' '+ str(datetime.now()) + ' ' + "target created successfully")
    #             conn.close()
    #             logging.info(' '+ str(datetime.now()) + ' ' +'postgres database connection closed after target creation')
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         logging.error(' '+ str(datetime.now()) + ' ' + error)
    #         print(error)
    # else:
    #     logging.error(' '+ str(datetime.now()) + ' ' + "target already exists")

    

def create_logging_tables(engine):
    """ create logging tables in the PostgreSQL database """
    commands = (
        """
        CREATE SCHEMA IF NOT EXISTS dbo
        """,
        """
        DROP TABLE IF EXISTS dbo.etl_jobs_logging CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.etl_jobs_logging (
            job_id INTEGER NOT NULL,
            job_name VARCHAR(50) NOT NULL,
            job_query VARCHAR(255) NOT NULL,
            active_flag CHAR(1)      
        )
        """,
        """ 
        DROP TABLE IF EXISTS dbo.etl_jobs_execution_logging CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.etl_jobs_execution_logging (
                job_id INTEGER NOT NULL,
                job_run_id VARCHAR(255),
                start_time TIMESTAMP,
                end_time TIMESTAMP, 
                status VARCHAR(20),
                error_message TEXT,
                last_updated_time TIMESTAMP
                )
        """
        )
    try:
        with engine.begin() as conn:
            # create table one by one
            for command in commands:
                conn.execute(command)

        jobs_orchestration_df = pd.read_csv('./references/jobs_orchestration.csv')
        jobs_orchestration_df.to_sql('etl_jobs_logging', con=engine, if_exists='replace', index=False, schema='dbo')

        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.error(' '+ str(datetime.now()) + ' ' + error)

def main():
    # B2B database
    database_name = 'target'

    engine = create_engine(
    f"postgresql://{DB_USER}:%s@{DB_HOST}:{DB_PORT}/{DB_NAME}" % urllib.parse.quote(DB_PASS))

    create_target_database(database_name, engine)

    engine = create_engine(
    f"postgresql://{DB_USER}:%s@{DB_HOST}:{DB_PORT}/{database_name}" % urllib.parse.quote(DB_PASS))

    create_logging_tables(engine)
    logging.info(' '+ str(datetime.now()) + ' ' + 'dbo.etl_jobs_logging, dbo.etl_jobs_execution_logging in target database created successfully\n')

if __name__ == "__main__":
    main()