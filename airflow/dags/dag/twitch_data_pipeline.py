import os
import yaml
import datetime
import pendulum
from pathlib import Path
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from ..ingestion.producer import *
from ..ingestion.consumer import kafka_consumer

with DAG(
    dag_id='twitch_dag',
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    def load_config(**kwargs):zj
    
        """Load env variables and config"""
        load_dotenv()
        server = os.environ.get('SERVER')
        port = int(os.environ.get('PORT'))
        nickname = os.environ.get('NICKNAME')
        token = os.environ.get('TOKEN')

        yaml_file_path = __file__.replace('.py', '.yaml')
        if Path(yaml_file_path).exists():
            with open(yaml_file_path) as yaml_file:
                config = yaml.safe_load(yaml_file)
        else:
            raise Exception(f'Missing {yaml_file_path} file.')

        kwargs['ti'].xcom_push(key='server', value=server)
        kwargs['ti'].xcom_push(key='port', value=port)
        kwargs['ti'].xcom_push(key='nickname', value=nickname)
        kwargs['ti'].xcom_push(key='token', value=token)
        kwargs['ti'].xcom_push(key='config', value=config)

    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        provide_context=True,
        dag=dag,
    )

    def run_get_twitch_stream(**kwargs):
        """Run get_twitch function"""
        server = kwargs['ti'].xcom_pull(key='server', task_ids='load_config')
        port = kwargs['ti'].xcom_pull(key='port', task_ids='load_config')
        nickname = kwargs['ti'].xcom_pull(key='nickname', task_ids='load_config')
        token = kwargs['ti'].xcom_pull(key='token', task_ids='load_config')
        config = kwargs['ti'].xcom_pull(key='config', task_ids='load_config')

        kp = kafka_producer(
            bootstrap_servers=config.get('topic1').get('bootstrap_servers'),
            server=server, port=port, nickname=nickname, token=token,
            channel=config.get('channel')
        )

        kp.get_twitch_stream(topic=config.get('topic1').get('name'))

    def run_process_messages(**kwargs):
        """Process twitch messages received from get_twitch function"""
        config = kwargs['ti'].xcom_pull(key='config', task_ids='load_config')

        kc = kafka_consumer(
            topic_name=config.get('topic1').get('name'),
            bootstrap_servers=config.get('topic1').get('bootstrap_servers'),
            auto_offset_reset='earliest'
        )

        kc.process_messages()

    task_process_messages = PythonOperator(
        task_id='process_messages',
        python_callable=run_process_messages,
        provide_context=True,
        trigger_rule='none_failed',  
        dag=dag,
    )

    task_get_twitch_stream = PythonOperator(
        task_id='get_twitch_stream',
        python_callable=run_get_twitch_stream,
        provide_context=True,
        dag=dag,
    )

load_config_task >> task_get_twitch_stream
task_get_twitch_stream >> task_process_messages
