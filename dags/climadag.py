from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.obtener_clima import get_weather
from src.transfor_data import transformacion_data
from src.subiendo_tabla import cargar_tabla

# Definiendo los argumentos del DAG
default_args = {
    'owner': 'charles',
    'depends_on_past': False,
    'email': ['sidebyside503@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Definir el DAG, la fecha de inicio y la frecuencia con que se ejecuta
dag = DAG(
    dag_id='climadag',
    default_args=default_args,
    start_date=datetime(2024, 4, 30),
    schedule_interval="0 0 * * *",  # Ejecuta a medianoche todos los días
    catchup=False  # Evitar la ejecución retrospectiva si la fecha de inicio es en el pasado
)

# La primera tarea es obtener el tiempo de openweathermap.org.
task1 = PythonOperator(
    task_id='obtener_clima',
    python_callable=get_weather,
    op_kwargs={'execution_date': '{{ ds }}', 'dag_id': '{{ dag.dag_id }}', 'task_id': '{{ task_instance.task_id }}'},
    dag=dag)

# Segunda tarea es transformar la data.
task2 = PythonOperator(
    task_id='transfor_data',
    python_callable=transformacion_data,
    op_kwargs={'execution_date': '{{ ds }}', 'dag_id': '{{ dag.dag_id }}', 'task_id': '{{ task_instance.task_id }}'},
    dag=dag)

# Tercera tarea es cargar la data en la base de datos.
task3 = PythonOperator(
    task_id='subiendo_tabla',
    python_callable=cargar_tabla,
    op_kwargs={'execution_date': '{{ ds }}', 'dag_id': '{{ dag.dag_id }}', 'task_id': '{{ task_instance.task_id }}'},
    dag=dag)

# Definiendo las dependencias: task1 debe completarse antes de que task2 pueda iniciarse y así sucesivamente.
task1 >> task2 >> task3
