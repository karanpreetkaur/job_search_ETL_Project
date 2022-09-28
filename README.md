# ETL Pipeline - Online Taxi Service
## 1.1 Setting up run environment

For this project, I have used `poetry` for dependency management. Dependencies are maintained in a poetry file `pyproject.toml`. To run the code in your workspace, follow these instructions:

Prequisites dependencies: `pip`, `pgadmin`

1. Install poetry via `pip install poetry` on the command line.

2. Clone github [repo](https://github.com/karanpreetkaur/online_taxi_service_ETL_Project) to your workspace.

3. Type `cd online_taxi_service_ETL_Project` in command line from directory where you cloned the repo.

4. Next, Type `poetry install` in command line. This will install all the dependencies required to run the code and create virtual environment to let us run with those dependencies.

5. To check if environment is created and activated, type `poetry env list`

6. Now, you need to setup postgres database credentials using .env file. You can edit below template for DB_PASS (postgres database password), DB_HOST, DB_USER, DB_NAME and DB_PORT.

    ```
    DB_USER=
    DB_PASS=
    DB_HOST=
    DB_PORT=
    DB_NAME=
    ```

7. To track the ETL process metadata, I have created logging which requires the below script to run.

   Run `poetry run python etl_logging.py` in cmd

8. To start the ETL process i.e. extract data from taxiservice database, weblogs, transform them and load into target.

   Run `poetry run python etl_job_run.py` in cmd

   - Any job failures will be recorded in dbo.etl_jobs_execution_logging in target datawarehouse and restart option will let jobs restart from the last failed job.

   - To restart the jobs, Run `poetry run python etl_job_run.py --run_type='restart'` in cmd

## ETL Process
- ### Online taxi service database
  - Online taxi service database consists of driver, car_model, cab, cab_ride, cab_ride_status, payment tables. These tables are created and populated with generated data by script `create_online_taxi_service_database.py`. In script, database connection is established using pyschopg and sqlalchemy and data is pushed to these tables.

- ### Weblogs
  - To generate weblogs in combined log format, I have used python script `create_weblogs.py` which saves logs in weblogs.log file.

- ### Transform and load weblog
  - The transformation on weblog requires to have country name and driver device name for each driver login.  
  - To get country name, I planned to use IP address first but then I could find API's who does with only few limited free requests. Hence, I have used country to timezone mapping and I'm using timezone to extract country name for that user login.
  - To extract device name, I have used string extraction methods
  - The script for the process is `transform_logs_load.py`. The script do above transformations and push data in tables to target datawarehouse.

  - `dbo.vw_top5_driver_login_device`: Displays most popular used devices for driver clients (top 5)


- ### Transform and load taxi service data
  - For reporting purposes, we can use driver and cab rides to data to prepare sample reports using `transform_taxiservice_load.py`such as:

  - `dbo.vw_working_driver_expirylicense`: Displays all active/working driver information whose driving license is expiring in next 1 year.
  - `dbo.vw_percentage_canceled_rides`: Displays percentage of cancelled rides by total rides.