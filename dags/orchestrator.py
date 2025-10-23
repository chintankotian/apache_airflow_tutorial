from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from apis.weather_api_requests import main



default_args = {
    'description':"Orchestrator DAG to fetch weather data and store in Postgres",
    'start_date':datetime(2025, 10, 23),
    'catchup':False,

}
dag = DAG(
    dag_id="orchestrator_dag",
    default_args=default_args,
    schedule=timedelta(minutes=1)
)


with dag:
    pythonWeatherTask = PythonOperator(
        task_id="fetch_and_store_weather_data", 
        python_callable=main)




