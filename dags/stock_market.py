from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from include.stock_market.tasks import _get_stock_prices, _store_prices, _get_formatted_csv

SYMBOL = 'NVDA'


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False, # Doesn't run all the tasks from the start_date up to today
    tags=['stock_market'], # Add labels

)
def stock_market():
    
    # Check if our API is available
    @task.sensor(
        poke_interval=30, # Check at every 30 seconsd
        timeout=300,
        mode='poke',
    )
    def is_api_available() -> PokeReturnValue:
        import requests

        # Conn created in Airflow admin UI
        api = BaseHook.get_connection('stock_api')

        # "extra_dejson" gets values from the JSON yo can put in the "extra"
        # field on Admin UI
        url = f'{api.host}{api.extra_dejson["endpoint"]}'
        print(url)

        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None

        return PokeReturnValue(
            is_done=condition,
            xcom_value=url, # Passing the URL to the next task
        )
    

    # We're using an operator because, later, we'll mix with a DockerOperator
    # to run spark jobs. To avoid tricky dependencies, we'll use this operator
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        
        # Method we'll call
        python_callable=_get_stock_prices,

        # Method parameters
        op_kwargs={
            # Everything between "{{ }}" are evaluated at runtime.
            # The name of this function is Templating
            'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}',
            'symbol': SYMBOL
        }
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={
            'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}',
        }
    )

    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app', # Built in README setup
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375', # Configured in docker-compose file
        network_mode='container:spark-master', # The same as the "spark-master" container, defined in docker-compose
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        },
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="format_prices") }}'
        }
    )


    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv


stock_market()