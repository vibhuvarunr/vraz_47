################################################################################################################################
# Description : BIC_AZSCCOFA062_hist  to fact_wes_stock                                                                        #
# Type : one time (history)                                                                                                    #
# Frequency: one time                                                                                                          #
# Author : Vibhu Varun Ravulapudi                                                                                              #
# Version : 1.0                                                                                                                #
# Version history: #                                                                                                           #
# ##############################################################################################################################
#   Version         Date            Author                     Changes                                                         #
#     1.0           19-09-2023   Vibhu Varun Ravulapudi      	Initial version                                                # 
################################################################################################################################

from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from utils.email_util import success_mail, failure_mail
import datetime
import pandas as pd
import json
import pendulum
from google.cloud import logging

logger = logging.Client().logger("dag_p_fact_wes_stock_history")

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

def run_data(sp_name):
    import pymsteams
    error = ""
    sp_name = sp_name
    service_account_id = get_service_account_id()
    r_table = "BIC_AZSCCOFA062_hist"
    env_var = "__ENV__"
    frequency = "history"
    dag_name  = "dag_p_fact_wes_stock_history"
    p_table = "fact_wes_stock"

    try:
        logger.log_text(f"Starting the execution of {sp_name} dag", severity='INFO')

        # Reading config
        try:
            path = "/home/airflow/gcs/dags/config/config.json"
            with open(path, 'r') as file:
                dict_config = json.load(file)
            mail_teams_config = dict_config["mail_teams_config"]
            mail_list = mail_teams_config['mailer_list']
            mail_subject = mail_teams_config['subject']
            teams_url = mail_teams_config['teams_url']
        except Exception as e:
            error = f"Error while loading the config file: {str(e)}"
            raise Exception(error)
        
        
        # Creating Connections and hooks
        try:
            hook = BigQueryHook(delegate_to=None, use_legacy_sql=False)
            cursor = hook.get_conn().cursor()
        except Exception as e:
            error += f"Error Getting BigQuery Connection: {str(e)}"
            raise Exception(error)

        try:
                    # Run the sp for p_table
                    sp_query = f"""CALL `raw_zone.{sp_name}` ();"""
                    start_time = datetime.datetime.now()
                    cursor.execute(sp_query)
                    end_time = datetime.datetime.now()
                    
        except Exception as e:
                    error += f"Error in loading sp sp_name , error is {e}"
                    raise Exception(error)
      
        #email = EmailOperator(
        #            task_id="email",
        #            to=mail_list,
        #            subject=f"[GCP-{env_var}]: Alert email for successful data loads for {p_table}",
        #            html_content=f"""<b>Run date:</b> {datetime.datetime.now()}  <br> <b>Environment:</b> {env_var}  <br> <b>DAG name:</b> {dag_name}  <br> <b>Frequency:</b> {frequency}  <br> <b>Job status:</b> Success  <br> <b>Source table name:</b> {r_table}  <br> <b>Target table name:</b> {p_table}""")
        #email.execute(dict())
        #success_mail(env_var, mail_list, dag_name, p_table, r_table, frequency)

    except Exception as e:
            #email = EmailOperator(
            #    task_id="email",
            #    to=mail_list,
            #    subject=f"[GCP-{env_var}]: Alert Mail for failed data loads for {p_table}",
            #    html_content=f"""<b>Run date:</b> {datetime.datetime.now()}  <br> <b>Environment:</b> {env_var}  <br> <b>DAG name:</b> {dag_name}  <br> <b>Frequency:</b> {frequency}  <br> <b>Job status:</b> Failed  <br> <b>Failure reason:</b> {e}  <br> <b>Source table name:</b> {r_table}  <br> <b>Target table name:</b> {p_table}""")
            #email.execute(dict())
            #failure_mail(env_var, mail_list, dag_name, p_table, r_table, frequency, e)
            
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

with DAG("dag_p_fact_wes_stock_history",
         catchup=False,
         max_active_runs= 1,
         start_date=pendulum.yesterday("America/Chicago"),
         schedule_interval = None
         ) as dag:
    
    run_batches = PythonOperator(
        task_id="run_batches",
        python_callable=run_data,
        op_kwargs={'sp_name': 'sp_fact_wes_stock_history'},
        provide_context=True)

    end_task = DummyOperator(task_id="end_task")
    
run_batches >> end_task