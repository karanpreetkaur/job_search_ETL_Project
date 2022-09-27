#!/usr/bin/env python

# Author: Karanpreet Kaur
# date: 2022-09-04

"""ETL Jobs orchestration

Usage: etl_job_run.py [--run_type =<run_type>]

Options:
--run_type =<run_type>  Optional argument  Option to have new run for all ETL jobs or restart the jobs with (new, restart) [default:new]
"""


import pandas as pd
import psycopg2
import logging
import uuid
from dotenv import load_dotenv
import os
from datetime import datetime
from docopt import docopt

opt = docopt(__doc__)

# Load environment file
load_dotenv()

# Read connection details
DB_HOST = os.environ.get("DB_HOST")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PORT = os.environ.get("DB_PORT")

def new_etl_job_run(conn):
    connection = psycopg2.connect(**conn)
    connection.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = connection.cursor()

    #Preparing query to create a database
    get_active_etl_jobs = """ SELECT * FROM etl_jobs_logging WHERE active_flag = 'Y' """
    cursor.execute(get_active_etl_jobs)
    active_jobs = cursor.fetchall()

    active_jobs = pd.DataFrame(active_jobs, columns=['job_id', 'job_name', 'job_query', 'active_flag'])

    for i in range(len(active_jobs)):
        job_run_id = str(uuid.uuid1()).replace('-', '')
        start_job_values = [str(active_jobs.iloc[i]['job_id']), job_run_id, str(datetime.now()), str(datetime.now())]

        try:
            cursor.execute("INSERT INTO etl_jobs_execution_logging values (%s, %s, %s, NULL, 'Running', NULL, %s)", start_job_values)
            print(active_jobs.iloc[i]['job_query'])
            r = os.system(active_jobs.iloc[i]['job_query'])

            if r == 1:
                update_job_values = [f"{active_jobs.iloc[i]['job_query']} failed", str(datetime.now()), str(active_jobs.iloc[i]['job_id']), job_run_id]
                cursor.execute("UPDATE etl_jobs_execution_logging SET status = 'Failed', error_message = %s , last_updated_time = %s WHERE job_id = %s AND job_run_id = %s", update_job_values)
                break

            elif r == 0:
                update_job_values = [str(datetime.now()), str(datetime.now()), str(active_jobs.iloc[i]['job_id']), job_run_id]
                cursor.execute("UPDATE etl_jobs_execution_logging SET end_time = %s, status = 'Succeeded', last_updated_time = %s WHERE job_id = %s AND job_run_id = %s", update_job_values)

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(' '+ str(datetime.now()) + ' ' + error)
            print(error)


def restart_etl_jobs(conn):
    connection = psycopg2.connect(**conn)
    connection.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = connection.cursor()

    #Preparing query to create a database
    get_last_failed_job_details = """ SELECT 
                                            job_id, 
                                            job_run_id 
                                      FROM 
                                        etl_jobs_execution_logging
                                     WHERE 
                                        status = 'Failed'
                                     ORDER BY
                                        last_updated_time DESC 
                                     LIMIT 1
                                  """
    cursor.execute(get_last_failed_job_details)
    failed_jobs = cursor.fetchall()

    rest_etl_jobs_query = f""" SELECT * FROM etl_jobs_logging WHERE active_flag = 'Y' AND job_id >= {failed_jobs[0][0]}"""
    cursor.execute(rest_etl_jobs_query)
    rest_etl_jobs = pd.DataFrame(cursor.fetchall(), columns=['job_id', 'job_name', 'job_query', 'active_flag'])


    for i in range(len(rest_etl_jobs)):
        if rest_etl_jobs.iloc[i]['job_id'] == failed_jobs[0][0]:
            job_run_id = failed_jobs[0][1]
        else:
            job_run_id = str(uuid.uuid4()).replace('-', '')
        start_job_values = [str(rest_etl_jobs.iloc[i]['job_id']), job_run_id, str(datetime.now()), str(datetime.now())]

        try:
            if rest_etl_jobs.iloc[i]['job_id'] == failed_jobs[0][0]:
                update_job_values = [str(datetime.now()), str(datetime.now()), str(rest_etl_jobs.iloc[i]['job_id']), job_run_id]
                cursor.execute("UPDATE etl_jobs_execution_logging SET end_time = %s, status = 'Restarted', error_message = NULL , last_updated_time = %s WHERE job_id = %s AND job_run_id = %s", update_job_values)
            else:
                cursor.execute("INSERT INTO etl_jobs_execution_logging values (%s, %s, %s, NULL, 'Running', NULL, %s)", start_job_values)

            print(rest_etl_jobs.iloc[i]['job_query'])
            r = os.system(rest_etl_jobs.iloc[i]['job_query'])

            if r == 1:
                update_job_values = [f"{rest_etl_jobs.iloc[i]['job_query']} failed", str(datetime.now()), str(rest_etl_jobs.iloc[i]['job_id']), job_run_id]
                cursor.execute("UPDATE etl_jobs_execution_logging SET status = 'Failed', error_message = %s , last_updated_time = %s WHERE job_id = %s AND job_run_id = %s", update_job_values)
                break

            elif r == 0:
                update_job_values = [str(datetime.now()), str(datetime.now()), str(rest_etl_jobs.iloc[i]['job_id']), job_run_id]
                cursor.execute("UPDATE etl_jobs_execution_logging SET end_time = %s, status = 'Succeeded', last_updated_time = %s WHERE job_id = %s AND job_run_id = %s", update_job_values)

        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(' '+ str(datetime.now()) + ' ' + error)
            print(error)


def main(run_type):
    # target database
    database_name = 'target'

    conn = {
            "host": DB_HOST,
            "dbname": database_name,
            "user": DB_USER,
            "password": DB_PASS,
            "port": DB_PORT,
            "options":'-c search_path=dbo'
        }

    if run_type is None:
        new_etl_job_run(conn)
    elif run_type == 'restart':
        restart_etl_jobs(conn)

if __name__ == "__main__":
    main(opt["--run_type"])