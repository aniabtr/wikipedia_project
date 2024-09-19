from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator 
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import extract_wikipedia_data, preprocess_wikipedia_data, write_wikepedia_data
#defining the DAG
dag=DAG(
    dag_id='wikipedia_flow',
    default_args={
        'owner':'Ania',
        'start_date':datetime(2024,8,21)
        },
    schedule_interval=None,
    catchup=False
)
#tasks to do: 
#1-extrction

extract_data_from_wikepedia= PythonOperator(
    task_id='extract_data_from_wikipedia', 
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs= { 'url': 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity'},
    dag=dag
)
#2-Cleaning and transformin
preprocess_wikipedia_data=PythonOperator(
    task_id='preprocess_wikipedia_data',
    provide_context=True,
    python_callable=preprocess_wikipedia_data,
    dag=dag
)
write_wikepedia_data=PythonOperator(
    task_id='write_wikepedia_data',
    python_callable=write_wikepedia_data,
    provide_context=True,
    dag=dag
)
extract_data_from_wikepedia >> preprocess_wikipedia_data>> write_wikepedia_data

#if __name__ == "__main__":
    # Manually call the function outside of Airflow
    #extract_wikipedia_data(
        #url='https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population'
    #)
