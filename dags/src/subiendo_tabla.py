from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import csv
import logging

def cargar_tabla(execution_date, dag_id='climadag', task_id='obtener_clima'):
    GCS_BUCKET = Variable.get('GCS_BUCKET')
    pg_hook = PostgresHook(postgres_conn_id='weatherdb_postgres_conn')

    dir_archivo_path = os.path.join('/opt/airflow/dags/climadag/obtener_clima', dag_id, task_id)
    archivo_fuente_nombre = f"{execution_date}.csv"
    source_full_path = os.path.join(dir_archivo_path, archivo_fuente_nombre)

    gcs = GCSHook('gcpAirflowLab')
    gcs_src_object = f"{dag_id}/{task_id}/{archivo_fuente_nombre}"

    # Intenta descargar el archivo CSV desde GCS
    try:
        gcs.download(bucket_name=GCS_BUCKET, object_name=gcs_src_object, filename=source_full_path)
        logging.info(f"Archivo descargado con éxito de GCS: gs://{GCS_BUCKET}/{gcs_src_object}")
    except Exception as e:
        logging.error(f"Error al descargar de GCS: {e}")
        return  # Salir si no se pudo descargar el archivo

    # Comprueba que el archivo existe y no está vacío antes de intentar leerlo
    if not os.path.exists(source_full_path) or os.path.getsize(source_full_path) == 0:
        logging.error(f"No se encontró el archivo esperado o está vacío: {source_full_path}")
        return  # Salir si el archivo está vacío o no existe

    # Abrir el archivo fuente CSV y leerlo
    with open(source_full_path, 'r') as inputfile:
        csv_reader = csv.reader(inputfile, delimiter=',')
        headers = next(csv_reader, None)  # Obtener encabezados
        if headers is None:
            logging.error("El archivo CSV está vacío y no contiene encabezados.")
            return
        
        for row in csv_reader:
            insert_cmd = """INSERT INTO weather (city, country, latitude, longitude, todays_date, humidity, pressure, min_temp, max_temp, temp, weather) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            pg_hook.run(insert_cmd, parameters=row)
            logging.info("Datos insertados en la base de datos.")
