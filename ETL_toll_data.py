from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'James Obafemi',
    'start_date': days_ago(0),
    'email': ['obafemijo@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Task 1.3 definition
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf  /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)

# Task 1.4 definition
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /finalassignment/vehicle-data.csv > /finalassignment/csv_data.csv',
    dag=dag,
)

# Task 1.5 definition
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d $"\t" -f5-7 --output-delimiter="," /finalassignment/tollplaza-data.tsv > /finalassignment/tsv_data.csv',
    dag=dag,
)

# Task 1.6 definition
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cat /finalassignment/payment-data.txt | tr -s " " |cut -d " " -f10,11 --output-delimiter="," > /finalassignment/fixed_width_data.csv',
    dag=dag,
)

# Task 1.7 definition
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste /finalassignment/csv_data.csv /finalassignment/tsv_data.csv /finalassignment/fixed_width_data.csv -s > /finalassignment/extracted_data.csv',
    dag=dag,
)

# Task 1.8 definition
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr [a-z] [A-Z] < /finalassignment/extracted_data.csv > /finalassignment/transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data