from datetime import timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import requests
import tarfile
import csv
import shutil
import os

# URL source des données
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'

# Chemin staging RELATIF au projet (parent du dossier dags)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
destination_path = os.path.join(BASE_DIR, "staging")
os.makedirs(destination_path, exist_ok=True)

# --- Fonctions du pipeline ---
def download_dataset():
    out_tgz = os.path.join(destination_path, "tolldata.tgz")
    resp = requests.get(source_url, stream=True, timeout=30)
    resp.raise_for_status()
    with open(out_tgz, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

def untar_dataset():
    tgz_path = os.path.join(destination_path, "tolldata.tgz")
    with tarfile.open(tgz_path, "r:gz") as tar:
        tar.extractall(path=destination_path)

def extract_data_from_csv():
    input_file = os.path.join(destination_path, "vehicle-data.csv")
    output_file = os.path.join(destination_path, "csv_data.csv")
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Numéro de véhicule anonymisé', 'Type de véhicule'])
        for line in infile:
            row = line.split(',')
            writer.writerow([row[0], row[1], row[2], row[3]])

def extract_data_from_tsv():
    input_file = os.path.join(destination_path, "tollplaza-data.tsv")
    output_file = os.path.join(destination_path, "tsv_data.csv")
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Nombre d\'essieux', 'ID de péage', 'Code de péage'])
        for line in infile:
            row = line.split('\t')
            writer.writerow([row[0], row[1], row[2]])

def extract_data_from_fixed_width():
    input_file = os.path.join(destination_path, "payment-data.txt")
    output_file = os.path.join(destination_path, "fixed_width_data.csv")
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Code de type de paiement', 'Code de véhicule'])
        for line in infile:
            writer.writerow([line[0:6].strip(), line[6:12].strip()])

def consolidate_data():
    csv_file = os.path.join(destination_path, "csv_data.csv")
    tsv_file = os.path.join(destination_path, "tsv_data.csv")
    fixed_width_file = os.path.join(destination_path, "fixed_width_data.csv")
    output_file = os.path.join(destination_path, "extracted_data.csv")

    with open(csv_file, 'r') as csv_in, open(tsv_file, 'r') as tsv_in, open(fixed_width_file, 'r') as fixed_in, open(output_file, 'w', newline='') as out_file:
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)

        writer.writerow([
            'Rowid', 'Timestamp', 'Numéro de véhicule anonymisé', 'Type de véhicule',
            'Nombre d\'essieux', 'ID de péage', 'Code de péage',
            'Code de type de paiement', 'Code de véhicule'
        ])

        next(csv_reader)  # skip headers
        next(tsv_reader)
        next(fixed_reader)

        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)

def transform_data():
    input_file = os.path.join(destination_path, "extracted_data.csv")
    output_file = os.path.join(destination_path, "transformed_data.csv")

    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Type de véhicule'] = row['Type de véhicule'].upper()
            writer.writerow(row)

# --- Config du DAG ---
default_args = {
    'owner': 'Votre nom',
    'start_date': pendulum.today('UTC'),
    'email': ['votre email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='BISETL_toll_data',
    default_args=default_args,
    description="Devoir final d'Apache Airflow",
    schedule='@daily',
    catchup=False,
)

download_task = PythonOperator(task_id='download_dataset', python_callable=download_dataset, dag=dag)
untar_task = PythonOperator(task_id='untar_dataset', python_callable=untar_dataset, dag=dag)
extract_csv_task = PythonOperator(task_id='extract_data_from_csv', python_callable=extract_data_from_csv, dag=dag)
extract_tsv_task = PythonOperator(task_id='extract_data_from_tsv', python_callable=extract_data_from_tsv, dag=dag)
extract_fixed_width_task = PythonOperator(task_id='extract_data_from_fixed_width', python_callable=extract_data_from_fixed_width, dag=dag)
consolidate_task = PythonOperator(task_id='consolidate_data', python_callable=consolidate_data, dag=dag)
transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)

download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
