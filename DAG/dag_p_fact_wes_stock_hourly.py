################################################################################################################################
# Description : raw_wes_stock  to fact_wes_stock                                                              #
# Type : SCD1                                                                                                                  #
# Frequency: Hourly                                                                                                            #
# Author : Vibhu Varun Ravulapudi                                                                                              #
# Version : 1.0                                                                                                                #
# Version history: #                                                                                                           #
# ##############################################################################################################################
# Version         Date            Author                     Changes                                                           #
#   1.0           18-09-2023      Vibhu Varun Ravulapudi     Initial version                                                   # 
################################################################################################################################

from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.email_operator import EmailOperator
from utils.email_util_new import success_mail, failure_mail,update_success_flag
import datetime
import pandas as pd
import json
import pendulum
from google.cloud import logging
import time
# from datetime import datetime

dag_name = "dag_p_fact_wes_stock_hourly"
frequency = "hourly"

logger = logging.Client().logger("dag_p_fact_wes_stock_hourly")

def get_backlog_batches(hook, domain_name, raw_dataset_name, audit_process_tracker_table, dependent_tables):
    sql_query = f"""SELECT MAX(BATCH_ID) as max_p_batch_id FROM {raw_dataset_name}.{audit_process_tracker_table}
                    where PROCESS='{domain_name}';"""
    df = hook.get_pandas_df(sql=sql_query)
    last_batch_id = df.iloc[0]['max_p_batch_id']
    print(last_batch_id)
    #if str(last_batch_id) == 'nan':
    if str(last_batch_id) == 'nan' or str(last_batch_id) == '<NA>':
        sql_query = f"""select MIN(BATCH_ID) - 1 as min_r_batch_id from {raw_dataset_name}.{audit_process_tracker_table}
                       where PROCESS = '{dependent_tables[0]}';"""
        min_batch_id_df = hook.get_pandas_df(sql=sql_query)
        last_batch_id = min_batch_id_df.iloc[0]['min_r_batch_id']
        print(last_batch_id)
    sql_query = f"""select DISTINCT BATCH_ID, DATA_DATETIME from {raw_dataset_name}.{audit_process_tracker_table}
                       where PROCESS = '{dependent_tables[0]}' and BATCH_ID > {last_batch_id} order by BATCH_ID, DATA_DATETIME asc;"""
    batches_dates_df = hook.get_pandas_df(sql=sql_query)
    # max_batch_id = max_batch_id_df.iloc[0]['max_r_batch_id']
    records = batches_dates_df.to_records(index=False)
    batches_dates = list(records)

    return batches_dates



def get_service_account_id():
    import google.auth
    from google.auth.transport import requests
    service_account_id = ""
    try:
        credentials, project_id = google.auth.default()

        credentials.refresh(requests.Request())
        service_account_id = credentials._service_account_email
        print(service_account_id)
    except Exception as e:
        error = f"Error while fetching  due to error: {str(e)}"
        logger.log_text(error, severity='ERROR')
        raise Exception(error)

    return service_account_id


def run_data():
    import pymsteams
    from framework.daily_tracker import upsert_count_daily_tracker
    error = ""

    # Assigning all constants
    raw_dataset_name = "raw_zone"
    r_table = "raw_wes_stock"
    p_table = "fact_wes_stock"
    env_var = "__ENV__"
    release="DR5"
    domain="Cofe"
    source="Airflow"
    table_col_name = "TABLE_NAME"
    dependent_col_name = "DEPENDENT_TABLE_NAME"
    frequency = "hourly"
    batch_id = 0
    service_account_id = get_service_account_id()
    dag_name  = "dag_p_fact_wes_stock_hourly"

    try:
        logger.log_text(
            f"Starting the execution of {p_table} dag", severity='INFO')
        # Reading config
        try:
            path = "/home/airflow/gcs/dags/config/config.json"
            with open(path, 'r') as file:
                dict_config = json.load(file)
            mail_teams_config = dict_config["mail_teams_config"]
            mail_list = mail_teams_config['mailer_list']
            mail_subject = mail_teams_config['subject']
            teams_url = mail_teams_config['teams_url']
            audit_table = dict_config['audit_table']
            audit_process_tracker_table = dict_config['audit_process_tracker_table']
        except Exception as e:
            error = f"Error while loading the config file: {str(e)}"
            raise Exception(error)

        # Load Config for on_off mechanism
        flag = False
        try:
            logger.log_text(f"Loading backlogs config file", severity='INFO')
            path = "/home/airflow/gcs/dags/config/backlogs_config.csv"
            df = pd.read_csv(path)
            if str(df[df["table_name"] == p_table].reset_index()["status"][0]) == "1":
                flag = True
            logger.log_text(f"Setting check backlog flag to {flag}")
        except Exception as e:
            logger.log_text(
                f"Error in loading backlogs config due to error: {e}", severity='INFO')
            logger.log_text(
                "Setting check backlog flag to False", severity='INFO')

        # Creating Connections and hooks
        try:
            hook = BigQueryHook(delegate_to=None, use_legacy_sql=False)
            cursor = hook.get_conn().cursor()
        except Exception as e:
            error += f"Error Getting BigQuery Connection: {str(e)}"
            raise Exception(error)

        # Reading Dependent tables
        try:
            logger.log_text(
                f"Loading dependent tables from {audit_table} for {p_table}", severity='INFO')
            sql_query = f"select {dependent_col_name} from {raw_dataset_name}.{audit_table} where {table_col_name} = '{p_table}'"
            df = hook.get_pandas_df(sql=sql_query)
            df[dependent_col_name] = df[dependent_col_name].apply(
                lambda x: x.split(","))
            dependent_tables = df[dependent_col_name][0]
        except Exception as e:
            error += f"Error loading Dependent tables for {p_table} from {audit_table} : {str(e)}"
            raise Exception(error)

        logger.log_text(
            f"Dependent tables for {p_table} are {dependent_tables}", severity='INFO')
        print(dependent_tables)

        # Getting dates
        logger.log_text(f"Getting backlog dates to be processed for {p_table} from {audit_process_tracker_table}",
                        severity='INFO')
        try:
            batches_dates = get_backlog_batches(hook, p_table, raw_dataset_name,
                                      audit_process_tracker_table, dependent_tables)
        except Exception as e:
            error += f"Error getting backlog dates from {audit_process_tracker_table} : {str(e)}"
            raise Exception(error)



        # processing sp for pending dates we got
        # for i in range(1, len(dates)):
        for batch_id, event_date in batches_dates:
            #event_date = datetime.datetime.strptime(str(event_date).split(".")[0], '%Y-%m-%dT%H:%M:%S.%f')
            print(str(event_date)[:-3])
            event_date = datetime.datetime.strptime(str(event_date)[:-3], '%Y-%m-%dT%H:%M:%S.%f')
            # batch_id += 1
            logger.log_text(
                f"Processing Batch Id : {batch_id}", severity='INFO')
            print("Processing batch id", batch_id)

            # each_date = dates[i]
            logger.log_text(
                f"Started the Processing of {p_table} for {event_date} for batch id {batch_id}", severity='INFO')

            try:
                # Run the sp for p_table
                logger.log_text(
                    f"Started loading data from {dependent_tables} to {p_table} by running SP for {batch_id}",
                    severity='INFO')
                sp_query = """CALL raw_zone.sp_fact_wes_stock({batch_id}, '{event_date}', '{service_account_id}');""".format(batch_id=batch_id,event_date=event_date,
                    service_account_id=service_account_id)

                start_time = datetime.datetime.now()
                cursor.execute(sp_query)
                end_time = datetime.datetime.now()
                logger.log_text(
                    f"Completed sp_fact_wes_stock for loading data from {dependent_tables} to {p_table} for {event_date} for batch id {batch_id}",
                    severity='INFO')

                # Writing into the audit process tracker with retry 
                done = False
                retry_count = 0
                time_sec=0
                while not (done): 
                    try:
                        print("inside audit insertion try block")
                        logger.log_text(f"Inserting the record in {audit_process_tracker_table} for {p_table} for {event_date}",severity='INFO')
                        insert_sql = f"""insert  into  {raw_dataset_name}.{audit_process_tracker_table} values('{p_table}',
                                        '{event_date}',{batch_id},'{end_time}','{start_time}','{end_time}')"""
                        cursor.execute(insert_sql)
                        done = True
                        
                    except Exception as ex:
                        print(done)                       
                        retry_count = retry_count + 1
                        time_sec=time_sec+60
                        print("retry_count",retry_count)
                        print("time_sec : ",time_sec)
                        if retry_count >= 4:
                          done = True
                          logger.log_text(f" Retry for batch id {batch_id} failed", severity='INFO')
                          print(f" Retry for batch id {batch_id} failed")
                          raise ex
                        logger.log_text(f" Retry for batch id {batch_id},no of retry= {retry_count}", severity='INFO')
                        print(f" Retry for batch id {batch_id},no of retry= {retry_count}")
                        time.sleep(time_sec)

                logger.log_text(
                    f"Data has been loaded to {p_table} for {event_date} for batch id {batch_id}", severity='INFO')
                print(f"Data has been loaded to {p_table} for {event_date}")
                # Run function to update daily count
                send_alert = False
                if datetime.datetime.now().hour == 23:
                    send_alert = True
                upsert_count_daily_tracker('p_supplychain', p_table, event_date, send_alert)

            except Exception as e:
                error += f"Error in loading sp_fact_wes_stock for {event_date}, error is {e}"
                raise Exception(error)

        #email = EmailOperator(
        #            task_id="email",
        #            to=mail_list,
        #            subject=f"[GCP-{env_var}]: Alert email for successful data loads for {p_table}",
        #            html_content=f"""<b>Run date:</b> {datetime.datetime.now()},  <br> <b>Environment:</b> {env_var},  <br> <b>DAG name:</b> {dag_name},  <br> <b>Frequency:</b> {frequency},  <br> <b>Job status:</b> Success,  <br> <b>Source table name:</b> {r_table},  <br> <b>Target table name:</b> {p_table}.""")
        #email.execute(dict())
        # success_mail(env_var, mail_list, dag_name, p_table, r_table, frequency)
        update_success_flag(dag_name,release,domain,source)
        
    except Exception as e:
            #email = EmailOperator(
            #    task_id="email",
            #    to=mail_list,
            #    subject=f"[GCP-{env_var}]: Alert Mail for failed data loads for {p_table}",
            #    html_content=f"""<b>Run date:</b> {datetime.datetime.now()},  <br> <b>Environment:</b> {env_var},  <br> <b>DAG name:</b> {dag_name},  <br> <b>Frequency:</b> {frequency},  <br> <b>Job status:</b> Failed,  <br> <b>Failure reason:</b> {e},  <br> <b>Source table name:</b> {r_table},  <br> <b>Target table name:</b> {p_table}.""")
            #email.execute(dict())
            failure_mail(env_var, dag_name, p_table, r_table, frequency, e,release,domain,source)
            x = pymsteams.connectorcard(teams_url)
            x.text(f"{e}")
            x.send()
            logger.log_text(f"Error is : {e}", severity='ERROR')
            logger.log_text(f"Sent Mail and Teams notification for this failure: {e}", severity='INFO')
            print(e)


default_args = {
    'email': ['__MAIL_ID__'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}


with DAG("dag_p_fact_wes_stock_hourly",
         catchup=False,
         max_active_runs= 1,
         start_date=pendulum.yesterday("America/Chicago"),
         schedule_interval="10 * * * *"
         ) as dag:
    run_batches = PythonOperator(
        task_id="run_batches",
        python_callable=run_data,
        provide_context=True)

    end_task = DummyOperator(task_id="end_task")
    run_batches >> end_task