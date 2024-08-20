from airflow.providers.postgres.hooks.postgres import PostgresHook
from math import ceil
import time


def bulk_insert(schema, table, columns, data, batch_size=5000, conn_id='main_db'):

    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    batch_data = [data[i*batch_size:(i+1)*batch_size] for i in range(ceil(len(data)/batch_size))]

    for bdata in batch_data:
        postgres_hook.insert_rows(table=f'{schema}.{table}', rows=bdata, target_fields=columns, batch_size=batch_size, commit_every=5000, autocommit=True)
        time.sleep(1)
