from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import requests
import json
from datetime import datetime
import os
import logging


def get_weather(execution_date, dag_id, task_id):
    API_KEY = Variable.get('OPEN_WEATHER_API_KEY')
    GCS_BUCKET = Variable.get('GCS_BUCKET')
    parameters = {'q': 'San Salvador,SV', 'appid': API_KEY}
    logging.info(f"API_KEY={API_KEY}")

    result = requests.get("http://api.openweathermap.org/data/2.5/weather", params=parameters)

    if result.status_code == 200:
        json_data = result.json()
        logging.info(f"API Response: {json_data}")

        file_name = f"{execution_date}.json"
        dir_path = os.path.join("/opt/airflow/dags", dag_id, task_id)

        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(dir_path, file_name)

        with open(file_path, 'w') as outputfile:
            json.dump(json_data, outputfile)

        gcs = GCSHook('gcpAirflowLab')
        gcs_dest_object = os.path.join(dag_id, task_id, file_name)
        gcs.upload(GCS_BUCKET, gcs_dest_object, file_path, mime_type='application/octet-stream')
        logging.info(f"File uploaded to GCS: gs://{GCS_BUCKET}/{gcs_dest_object}")
    else:
        raise ValueError("Error al llamar la API.")
