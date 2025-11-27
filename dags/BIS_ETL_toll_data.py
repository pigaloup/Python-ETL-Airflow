from datetime import timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import requests
import tarfile
import csv
import shutil

# Définir le chemin pour les fichiers d'entrée et de sortie
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
destination_path = '/home/papidiop7/airflow/dags/python_etl/staging'


# Fonction pour télécharger le jeu de données
def download_dataset():
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(f"{destination_path}/tolldata.tgz", 'wb') as f:
            f.write(response.raw.read())
    else:
        print("Échec du téléchargement du fichier")


# Fonction pour extraire le jeu de données
def untar_dataset():
    with tarfile.open(f"{destination_path}/tolldata.tgz", "r:gz") as tar:
        tar.extractall(path=destination_path)


# Fonction pour extraire des données à partir de CSV
def extract_data_from_csv():
    input_file = f"{destination_path}/vehicle-data.csv"
    output_file = f"{destination_path}/csv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Numéro de véhicule anonymisé', 'Type de véhicule'])
        for line in infile:
            row = line.split(',')
            writer.writerow([row[0], row[1], row[2], row[3]])


# Fonction pour extraire des données à partir de TSV
def extract_data_from_tsv():
    input_file = f"{destination_path}/tollplaza-data.tsv"
    output_file = f"{destination_path}/tsv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Nombre d\'essieux', 'ID de péage', 'Code de péage'])
        for line in infile:
            row = line.split('\t')
            writer.writerow([row[0], row[1], row[2]])


# Fonction pour extraire des données à partir d'un fichier à largeur fixe
def extract_data_from_fixed_width():
    input_file = f"{destination_path}/payment-data.txt"
    output_file = f"{destination_path}/fixed_width_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Code de type de paiement', 'Code de véhicule'])
        for line in infile:
            writer.writerow([line[0:6].strip(), line[6:12].strip()])


# Fonction pour consolider les données
def consolidate_data():
    csv_file = f"{destination_path}/csv_data.csv"
    tsv_file = f"{destination_path}/tsv_data.csv"
    fixed_width_file = f"{destination_path}/fixed_width_data.csv"
    output_file = f"{destination_path}/extracted_data.csv"

    with open(csv_file, 'r') as csv_in, open(tsv_file, 'r') as tsv_in, open(fixed_width_file, 'r') as fixed_in, open(output_file, 'w') as out_file:
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)

        writer.writerow([
            'Rowid', 'Timestamp', 'Numéro de véhicule anonymisé', 'Type de véhicule',
            'Nombre d\'essieux', 'ID de péage', 'Code de péage',
            'Code de type de paiement', 'Code de véhicule'
        ])

        next(csv_reader)
        next(tsv_reader)
        next(fixed_reader)

        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)


# Fonction pour transformer les données
def transform_data():
    input_file = f"{destination_path}/extracted_data.csv"
    output_file = f"{destination_path}/transformed_data.csv"

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Type de véhicule'] = row['Type de véhicule'].upper()
            writer.writerow(row)


# Arguments par défaut pour le DAG
default_args = {
    'owner': 'Votre nom',
    'start_date': pendulum.today('UTC'),   # ✅ remplacement de days_ago
    'email': ['votre email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
dag = DAG(
    dag_id='BISETL_toll_data',
    default_args=default_args,
    description="Devoir final d'Apache Airflow",
    schedule='@daily',   # ✅ remplacement de schedule_interval
    catchup=False,
)

# Définir les tâches
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Définir les dépendances des tâches
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task

