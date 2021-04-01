from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime
import dbt
import pandas as pd

def _training_model(ti):
    accuracy = randint(1,10)
    df = pd.read_csv('./data/Boston/listings.csv')
    print(df.to_string()) 
    ti.xcom_push(key="model_accuracy", value=accuracy)

with DAG("airbnb", start_date=datetime(2021, 1,1),
    schedule_interval="@daily", catchup=False) as dag:
        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        training_model_A