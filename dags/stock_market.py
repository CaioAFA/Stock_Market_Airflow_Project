from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from datetime import datetime


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
        api = BaseHook.get_connection('stock_market')

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
    

    is_api_available()


stock_market()