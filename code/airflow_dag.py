########################################################################################
# Script name: airflow_dag.py
# Description: This script stores Airflow DAG logics.
# Creator: Travis Hong
# Repository: https://github.com/TravisH0301/global_warming_disaster
########################################################################################

# Import libraries
import datetime

from airflow import DAG
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.operators.python_operator import PythonOperator


# Define DAG
with DAG(
    dag_id="global_warming_disaster_data_pipeline",
    default_args={
        "owner": "airflow",
        "start_date": datetime.datetime(2022, 11, 20),
        "retries": 0
    },
    description="DAG to trigger a Lambda function followed by Glue Spark jobs for Global Warming Disaster analysis.",
    schedule_interval=datetime.timedelta(days=30),
    catchup=False,
) as dag:

    # Define functions to trigger lambda function and Glue jobs
    def invoke_lambda_function(function_name, region):
        lambda_hook = LambdaHook(
            region_name=region,
        )
        lambda_hook.invoke_lambda(
            function_name=function_name,
            log_type="Tail",
            payload="{}"
        )
        return f"Lambda function {function_name} started."

    def start_glue_job(job_name, region):
        glue_hook = GlueJobHook(
            job_name=job_name,
            region_name=region
        )
        glue_hook.initialize_job()
        return f"Glue Job {job_name} started."

    # Define variables
    region = "ap-southeast-2"
    lambda_function_name = "ingress-function"
    glue_job_names = [
        "ingress-to-bronze",
        "bronze-to-silver",
        "silver-to-gold"
    ]

    # Define tasks
    ingress_loader_task = PythonOperator(
        task_id="ingress_loader_task",
        provide_context=True,
        python_callable=invoke_lambda_function,
        op_kwargs={
            "function_name": lambda_function_name,
            "region": region
        },
        dag=dag,
    )

    ingress_to_bronze_task = PythonOperator(
        task_id="ingress_to_bronze_task",
        provide_context=True,
        python_callable=start_glue_job,
        op_kwargs={
            "job_name": glue_job_names[0],
            "region": region
        },
        dag=dag,
    )

    bronze_to_silver_task = PythonOperator(
        task_id="bronze_to_silver_task",
        provide_context=True,
        python_callable=start_glue_job,
        op_kwargs={
            "job_name": glue_job_names[1],
            "region": region
        },
        dag=dag,
    )

    silver_to_gold_task = PythonOperator(
        task_id="silver_to_gold_task",
        provide_context=True,
        python_callable=start_glue_job,
        op_kwargs={
            "job_name": glue_job_names[2],
            "region": region
        },
        dag=dag,
    )

    # Set task dependencies
    ingress_loader_task >> ingress_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task
