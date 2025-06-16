from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='alquiler_automovil',
    default_args=args,
    description='Realiza proceso etl de datos de alquileres de automoviles',
    schedule_interval='0 0 * * *',
    start_date=days_ago(0),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform', 'alquiler','automovil'],
    params={"example_key": "example_value"},
) as dag:

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    

    
    envio_mail= EmailOperator( 
                  task_id='envio_mail', 
                  to='matybustamante151@gmail.com', 
                  subject='Notificacion Mail', 
                  html_content = """
					<html>
					  <body style="font-family: Arial, sans-serif; color: #333;">
					    <h2 style="color: #2e6c80;">Notificacion de Airflow</h2>
					    <p>Hola,</p>
					    <p>Este es un aviso automatico del DAG <strong>{{ dag.dag_id }}</strong>.</p>
					    <p><strong>Tarea:</strong> {{ task_instance.task_id }}</p>
					    <p><strong>Estado:</strong> {{ task_instance.state }}</p>
					    <p><strong>Fecha de ejecucion </strong> {{ execution_date }}</p>

					    <hr>
					    <p style="font-size: 12px; color: #888;">
					      Este mensaje fue generado automaticamente por Apache Airflow.
					    </p>
					  </body>
					</html>
					""", 
                  dag=dag
              )

    ingest = BashOperator(
        task_id='ingesta',
        bash_command='/usr/bin/sh /home/hadoop/scripts/examen_final/ejercicio_2/ingest_alquiler_automovil.sh ',
    )

    transformacion = BashOperator(
        task_id='transformacion',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/examen_final/ejercicio_2/alquiler_automovil.py ',
    )

    
    ingest >> transformacion >>  envio_mail >> finaliza_proceso 




if __name__ == "__main__":
    dag.cli()
