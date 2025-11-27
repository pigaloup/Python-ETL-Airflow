from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data_engineer',
    'start_date': days_ago(0),
    'email': ['data.engineer@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Devoir final Apache Airflow',
    schedule_interval=timedelta(days=1),
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv | tr '\\t' ',' > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv
    """,
    dag=dag
)


extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    awk '{print $(NF-1) "," $NF}' /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv
    """,
    dag=dag
)


consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv',
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    cut -d',' -f1-3 /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /tmp/left.csv
    cut -d',' -f4 /home/project/airflow/dags/finalassignment/staging/extracted_data.csv | tr '[:lower:]' '[:upper:]' > /tmp/middle.csv
    cut -d',' -f5-9 /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /tmp/right.csv
    paste -d',' /tmp/left.csv /tmp/middle.csv /tmp/right.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv
    """,
    dag=dag
)

# Définition des dépendances
unzip_data >> extract_data_from_csv
unzip_data >> extract_data_from_tsv
unzip_data >> extract_data_from_fixed_width

extract_data_from_csv >> consolidate_data
extract_data_from_tsv >> consolidate_data
extract_data_from_fixed_width >> consolidate_data

consolidate_data >> transform_data
