from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime

def _training_model(ti):
    accuracy = randint(1,10)
    ti.xcom_push(key="model_accuracy", value=accuracy)

def _print_hello():
    print("Hello")

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(key="model_accuracy", task_ids=[
        "training_model_A",
        "training_model_B",
        "training_model_C"
    ])
    print(accuracies)
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return "accurate"
    return "inaccurate"

with DAG("my_dag2", start_date=datetime(2021, 1,1),
    schedule_interval="@daily", catchup=False) as dag:
        training_model_A = PythonOperator(
            task_id="training_model_A",
            python_callable=_training_model
        )

        training_model_B = PythonOperator(
            task_id="training_model_B",
            python_callable=_training_model
        )
        

        training_model_C = PythonOperator(
            task_id="training_model_C",
            python_callable=_training_model
        )
        
        choose_best_model = BranchPythonOperator(
            task_id ="choose_best_model",
            python_callable = _choose_best_model
        )

        accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'",
            do_xcom_push=False
        )

        inaccurate = BashOperator(
            task_id="inaccurate",
            bash_command="echo 'inaccurate'",
            do_xcom_push= False

        )

        hello_operator = PythonOperator(
            task_id='hello_operator', 
            python_callable=_print_hello,
            do_xcom_push=False
        )

        [training_model_B, training_model_A, training_model_C] >> choose_best_model >> [accurate, inaccurate]
        inaccurate >> hello_operator