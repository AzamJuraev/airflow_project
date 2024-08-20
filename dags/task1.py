from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils import db_utils
from airflow import DAG
import requests


class JSONFile:
    def __init__(self):
        self.now = datetime.now(dag.timezone).date()
        self.postgres_hook = PostgresHook(postgres_conn_id='main_db')

    def make_request(self):
        response = requests.get(url='https://drive.google.com/uc?export=download&id=1vbHELLhLzr5VaDiaeesDQ6BRsEoFd9lK')
        if response.status_code == 200:
            return response.json()
        else:
            return None


    def main_func(self):
        data_collection = []
        json_data = self.make_request()

        if len(json_data['data']) > 0:
            for each_data in json_data["data"]:
                application_id = each_data.get('application_id', None)
                publisher_name = each_data.get('publisher_name', None)
                publisher_id = each_data.get('publisher_id', None)
                tracker_name = each_data.get('tracker_name', None)
                tracking_id = each_data.get('tracking_id', None)
                click_timestamp = each_data.get('click_timestamp', None)
                click_datetime = each_data.get('click_datetime', None)
                click_ipv6 = each_data.get('click_ipv6', None)
                click_url_parameters = each_data.get('click_url_parameters', None)
                click_id = each_data.get('click_id', None)
                click_user_agent = each_data.get('click_user_agent', None)
                ios_ifa = each_data.get('ios_ifa', None)
                ios_ifv = each_data.get('ios_ifv', None)
                android_id = each_data.get('android_id', None)
                google_aid = each_data.get('google_aid', None)
                os_name = each_data.get('os_name', None)
                os_version = each_data.get('os_version', None)
                device_manufacturer = each_data.get('device_manufacturer', None)
                device_model = each_data.get('device_model', None)
                device_type = each_data.get('device_type', None)
                is_bot = each_data.get('is_bot', None)
                country_iso_code = each_data.get('country_iso_code', None)
                city = each_data.get('city', None)

                data_collection.append((application_id, publisher_name, publisher_id, tracker_name, tracking_id, click_timestamp, click_datetime, click_ipv6, click_url_parameters, click_id, click_user_agent,
                                        ios_ifa, ios_ifv, android_id, google_aid, os_name, os_version, device_manufacturer, device_model, device_type, is_bot, country_iso_code, city))

        columns = ["application_id", "publisher_name", "publisher_id", "tracker_name", "tracking_id", "click_timestamp", "click_datetime", "click_ipv6", "click_url_parameters", "click_id", "click_user_agent",
                                        "ios_ifa", "ios_ifv", "android_id", "google_aid", "os_name", "os_version", "device_manufacturer", "device_model", "device_type", "is_bot", "country_iso_code", "city"]

        db_utils.bulk_insert(schema='public', table='clicks_data', data=data_collection, columns=columns, batch_size=5000)


default_args = {
    'owner': 'azam',
    'depends_on_past': False
}


with DAG(
    dag_id='json_data',
    start_date=datetime(2024, 8, 16),
    catchup=False,
    schedule_interval='0 23 * * *',
    default_args=default_args,
    description='Load and transform data in json_data with Airflow'
) as dag:
    task = PythonOperator(
        task_id="main_func",
        python_callable=JSONFile().main_func,
        dag=dag)

