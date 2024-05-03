from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowException
import os
import json
import numpy as np
import csv
import logging

def transformacion_data(execution_date, dag_id='climadag', task_id='obtener_clima'):
    GCS_BUCKET = Variable.get('GCS_BUCKET')
    
    # Construye la ruta al archivo JSON de manera correcta
    source_dir_path = os.path.join('/opt/airflow/dags', dag_id, task_id)
    source_file_name = f"{execution_date}.json"
    source_full_path = os.path.join(source_dir_path, source_file_name)

    gcs = GCSHook('gcpAirflowLab')
    gcs_src_object = os.path.join(dag_id, task_id, source_file_name)
    try:
        local_file = gcs.download(bucket_name=GCS_BUCKET, object_name=gcs_src_object, filename=source_full_path)
        logging.info(f"Archivo descargado con éxito de GCS: gs://{GCS_BUCKET}/{gcs_src_object}")
        with open(local_file, 'r') as inputfile:
            doc = json.load(inputfile)
        process_and_write_data(doc, execution_date, dag_id, task_id)
    except AirflowException as e:
        logging.error(f"Error al acceder a Google Cloud Storage: {e}")
    except Exception as e:
        logging.error(f"Error general en transformación de datos: {e}")

def process_and_write_data(doc, execution_date, dag_id, task_id):
    try:
        logging.info("Procesando datos recibidos...")
        city = doc.get('name', 'Unknown')
        country = doc.get('sys', {}).get('country', 'Unknown')
        lat = float(doc['coord']['lat'])
        lon = float(doc['coord']['lon'])
        humid = float(doc['main']['humidity'])
        press = float(doc['main']['pressure'])
        min_temp = float(doc['main']['temp_min']) - 273.15
        max_temp = float(doc['main']['temp_max']) - 273.15
        temp = float(doc['main']['temp']) - 273.15
        weather = doc['weather'][0]['description']
        todays_date = execution_date.split('T')[0]

        row = [city, country, lat, lon, todays_date, humid, press, min_temp, max_temp, temp, weather]
        logging.info(f"Datos procesados: {row}")

        dest_file_name = f"{todays_date}.csv"
        dest_dir_path = os.path.join('/opt/airflow/dags', dag_id, task_id)
        if not os.path.exists(dest_dir_path):
            os.makedirs(dest_dir_path, exist_ok=True)
            logging.info(f"Directorio creado: {dest_dir_path}")

        dest_full_path = os.path.join(dest_dir_path, dest_file_name)
        with open(dest_full_path, 'w', newline='') as csvfile:
            fieldnames = ['City', 'Country', 'Latitude', 'Longitude', 'Date', 'Humidity', 'Pressure', 'Min Temp', 'Max Temp', 'Temperature', 'Weather']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow({'City': city, 'Country': country, 'Latitude': lat, 'Longitude': lon, 'Date': todays_date, 'Humidity': humid, 'Pressure': press, 'Min Temp': min_temp, 'Max Temp': max_temp, 'Temperature': temp, 'Weather': weather})

        logging.info(f"Datos escritos correctamente en {dest_full_path}")
    except Exception as e:
        logging.error(f"Error durante el procesamiento de datos: {e}")
        raise
if __name__ == "__main__":
    transformacion_data('2024-05-01')