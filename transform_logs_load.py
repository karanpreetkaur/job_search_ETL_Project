#!/usr/bin/env python

# Author: Karanpreet Kaur
# date: 2022-09-05

"""Transform and Load weblogs data"""
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import urllib.parse
from sqlalchemy import create_engine

# Load environment file
load_dotenv()

# Read connection details
DB_HOST = os.environ.get("DB_HOST")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PORT = os.environ.get("DB_PORT")

def transform_weblogs():
    """ Transformation of weblogs for reporting """

    # Read weblogs data 
    weblogs_data = pd.read_csv('weblogs.log', sep=" ", header=None)
    weblogs_data.columns = ['ip_address', 'identity_of_client', 'user_name', 'timestamp', 'timezone', 'http_request', 'http_status_code', 'bytes_transferred', 'referer', 'user_agent']

    # Select relevant columns required for reporting
    relevant_columns = ['ip_address', 'user_name', 'timestamp', 'timezone', 'user_agent']
    weblogs_data = weblogs_data[relevant_columns]

    # Do transformation of timestamp and refine timezone
    weblogs_data['timestamp'] = weblogs_data['timestamp'] + ' ' + weblogs_data['timezone']
    weblogs_data['timezone'] = weblogs_data['timezone'].str.replace(']', '')
    weblogs_data['timezone'] =  'UTC '+ (weblogs_data['timezone'].str.slice(0, 3) + ':' + weblogs_data['timezone'].str.slice(3, 6))
    weblogs_data['timezone'] =  weblogs_data['timezone'].replace('UTC +00:00', 'UTC').replace('UTC -00:00', 'UTC')

    # Read country time zone mapping to map countries for weblogs
    country_timezone_mapping = pd.read_csv('./references/country_time_zone.csv')
    country_timezone_mapping.columns = ['country', 'city', 'utc_offset']

    # Sampling one country per utc offset
    country_timezone_mapping = country_timezone_mapping.groupby('utc_offset').apply(lambda x: x.sample(1)).reset_index(drop=True)

    # Get country name per user login based on timezone(utc offset)
    weblogs_country_data = weblogs_data.merge(country_timezone_mapping[['country', 'utc_offset']], left_on='timezone', right_on='utc_offset', how='left')
    weblogs_country_data.drop(columns=['utc_offset'], inplace=True)

    # Get client device names
    weblogs_country_data['client_device'] = weblogs_country_data['user_agent'].str.split('(').str[1].str.split(')').str[0].str.split(';').str[0]
    weblogs_country_data.drop(columns=['user_agent'], inplace=True)
    weblogs_country_data['client_device'].fillna('Unknown', inplace=True)
    weblogs_country_data['country'].fillna('Unknown', inplace=True)

    return weblogs_country_data

def load_logs_to_dw(transformed_weblogs, engine):
    """Load transformed weblogs into target datawarehouse in dbo schema"""
    command = (
        """
        DROP TABLE IF EXISTS dbo.driver_weblogs CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.driver_weblogs (
            ip_address VARCHAR(50) NOT NULL,
            user_name VARCHAR(50) NOT NULL,
            timestamp VARCHAR(255) NOT NULL,
            timezone VARCHAR(20) NOT NULL,
            country VARCHAR(70) NOT NULL,
            client_device CHAR(50) NOT NULL
        )
        """,
        """
        CREATE OR REPLACE VIEW dbo.vw_top5_driver_login_device AS
        SELECT 
            client_device AS driver_login_device_name, 
	        COUNT(*) AS n_logins
        FROM dbo.user_weblogs
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 5 
        """
    )

    with engine.begin() as conn:
        conn.execute(command[0])
    transformed_weblogs.to_sql('user_weblogs', con=engine, if_exists='replace', index = False, schema='dbo')

    with engine.begin() as conn:
        conn.execute(command[1])

def main():
    # target database
    database_name = 'target'

    engine = create_engine(
    f"postgresql://{DB_USER}:%s@{DB_HOST}:{DB_PORT}/{database_name}" % urllib.parse.quote(DB_PASS))

    transformed_weblogs = transform_weblogs()
    load_logs_to_dw(transformed_weblogs, engine)

if __name__ == "__main__":
    main()