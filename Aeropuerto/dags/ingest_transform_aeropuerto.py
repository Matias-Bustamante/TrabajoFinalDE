from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='examen_final_aeropuerto',
    default_args=args,
    description='Realiza proceso etl de datos de aeropuerto de argentina',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform', 'aeropuerto'],
    params={"example_key": "example_value"},
) as dag:

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    


    ingest = BashOperator(
        task_id='ingesta',
        bash_command='/usr/bin/sh /home/hadoop/scripts/examen_final/ejercicio_1/ingest_aeropuerto.sh ',
    )

    transformacion = BashOperator(
        task_id='transformacion',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio_1/transform_aeropuerto.py ',
    )

    
    ingest >> transformacion >>  finaliza_proceso




if __name__ == "__main__":
    dag.cli()
