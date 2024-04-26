
import logging
import datetime
import requests
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData


logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')

engine.execute(('create schema if not exists stage'))
engine.execute(('create schema if not exists comics'))
metadata = MetaData(schema='stage')
comic_table = Table(
    'comic', metadata,
    Column('month', Integer),
    Column('num', Integer, primary_key = True),
    Column('link', String(1000)),
    Column('year', Integer),
    Column('news', String(1000)),
    Column('safe_title', String(1000)),
    Column('transcript', Text),
    Column('alt', Text),
    Column('img', String(1000)),
    Column('title', String(1000)),
    Column('day', Integer)
)

with DAG(
    "Load_Raw_Comics",
    description="Load Raw Comics",
    schedule='0 0 * * MON,WED,FRI',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False
) as dag:

    def get_last_comic(ti):
        metadata.create_all(engine)
        with engine.connect() as conn:
            res = conn.execute('select max(num) from stage.comic')
            last_comic_num = res.fetchone()[0]
        ti.xcom_push(key='last_comic', value=int(last_comic_num or 2900))
    
    def check_new_comic(ti):
        last_comic = ti.xcom_pull(key='last_comic', task_ids=['get_last_comic'])[0]
        current_comic = int(requests.get('https://xkcd.com/info.0.json').json()['num'])
        if last_comic < current_comic:
            logger.info('New comics loaded to server')
            ti.xcom_push(key='new_comic', value=current_comic)
            return True

    def get_new_comics(last, new):
        table_cols = list(map(lambda x: str(x).split('.')[1], comic_table.columns))
        print(table_cols)
        comic_list = []
        for i in range(last, new + 1):
            comic_list.append({k: dict(requests.get(f'https://xkcd.com/{i}/info.0.json').json())[k] for k in table_cols})
        return comic_list

    def load_comic_to_db(ti):
        last_comic = ti.xcom_pull(key='last_comic', task_ids=['get_last_comic'])[0]
        new_comic = ti.xcom_pull(key='new_comic', task_ids=['wait_for_new_comic'])[0]
        comics_list = get_new_comics(last_comic, new_comic)
        with engine.connect() as conn:
            insert_statement = comic_table.insert().values(comics_list)
            print(insert_statement)
            conn.execute(insert_statement)
        logger.info('New comics loaded to database')


    get_last_comic_task = PythonOperator(
        task_id="get_last_comic",
        python_callable=get_last_comic,
        do_xcom_push=False
    )

    wait_for_new_comic = PythonSensor(
        task_id='wait_for_new_comic',
        python_callable=check_new_comic,
        poke_interval=600,
        mode='reschedule',
        do_xcom_push=False
    )

    load_comic_to_db_task = PythonOperator(
        task_id="load_comic_to_db",
        python_callable=load_comic_to_db,
        do_xcom_push=False
    )

    transform_comics = BashOperator(
        task_id="transform_comics",
        bash_command="cd /opt/app/dbt_comics && dbt run -s fact_comics && dbt run && dbt test"
    )


get_last_comic_task >> wait_for_new_comic >> load_comic_to_db_task >> transform_comics
