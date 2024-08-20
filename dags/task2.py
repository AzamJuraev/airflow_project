from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils import db_utils
from airflow import DAG
import requests
import json


class FoodFacts:
    def __init__(self):
        self.now = datetime.now(dag.timezone).date()
        self.postgres_hook = PostgresHook(postgres_conn_id='main_db')

    def make_request(self, page):
        params = {
                    "fields": "product_name,code,nutrition_grades,categories_tags_en,brands,image_url",
                    "page": page,
                    "page_size": 100
        }
        response = requests.get(url='https://world.openfoodfacts.net/api/v2/search', params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return None


    def get_data_to_tmp_table(self, **kwargs):
        ti = kwargs['ti']
        with open('/opt/airflow/dags/current_page.json', 'r') as file:
            data = json.load(file)
        current_page = data['current_page']

        data_collection = []
        columns = ["brands", "code", "product_name", "image_url", "nutrition_grades", "categories_tags_en"]

        for each_page in range(current_page+1, current_page + 11):
            response_data = self.make_request(page=each_page)
            if response_data:
                if len(response_data['products']) > 0:
                    for each_product in response_data['products']:
                        brands = each_product.get('brands', None)
                        code = each_product.get('code', None)
                        product_name = each_product.get('product_name', None)
                        image_url = each_product.get('image_url', None)
                        nutrition_grades = each_product.get('nutrition_grades', None)
                        categories_tags_en = each_product.get('categories_tags_en', None)

                        data_collection.append((brands, code, product_name, image_url, nutrition_grades, categories_tags_en))

        db_utils.bulk_insert(schema='public', table='food_info_tmp', data=data_collection, columns=columns, batch_size=5000)
        ti.xcom_push(key='last_page', value=each_page)


    def pagination_file_update(self, **kwargs):
        ti = kwargs['ti']
        last_page = ti.xcom_pull(task_ids='get_data_to_tmp_table', key='last_page')

        with open('/opt/airflow/dags/current_page.json', 'r') as file:
            data = json.load(file)

        data['current_page'] = last_page

        with open('/opt/airflow/dags/current_page.json', 'w') as file:
            json.dump(data, file, indent=4)



default_args = {
    'owner': 'azam',
    'depends_on_past': False
}


with DAG(
    dag_id='food_facts',
    start_date=datetime(2024, 8, 16),
    catchup=False,
    schedule_interval='*/1 * * * *',
    default_args=default_args,
    description='Load and transform data in food_facts with Airflow'
) as dag:
    task = PostgresOperator(
        task_id='tmp_cleaner',
        sql='truncate table public.food_info_tmp',
        postgres_conn_id='main_db',
        dag=dag)
    task1 = PythonOperator(
        task_id="get_data_to_tmp_table",
        python_callable=FoodFacts().get_data_to_tmp_table,
        dag=dag)
    task2 = PostgresOperator(
        task_id="load_categories",
        postgres_conn_id="main_db",
        sql="sql/load_categories.sql")
    task3 = PostgresOperator(
        task_id="load_products",
        postgres_conn_id="main_db",
        sql="sql/load_products.sql")
    task4 = PythonOperator(
        task_id="pagination_file_update",
        python_callable=FoodFacts().pagination_file_update,
        dag=dag)


    task >> task1 >> task2 >> task3 >> task4
