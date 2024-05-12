import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json

# Source websites to scrape
sources = {
    'dawn': 'https://www.dawn.com/',
    'bbc': 'https://www.bbc.com/'
}

# Global list to store extracted data
articles = []

# Data extraction task
def extract():
    for name, url in sources.items():
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, 'html.parser')
        urls = []
        for link in soup.find_all('a', href=True):
            # Store only relevant article links
            article_url = link['href']
            if article_url.startswith('/') or article_url.startswith(url):
                full_url = article_url if article_url.startswith('http') else url + article_url.lstrip('/')
                urls.append(full_url)

        # Add collected links to global articles list
        articles.append({
            'source': name,
            'urls': urls
        })

# Data transformation task
def transform():
    # Example transformation: print the URLs found
    for source_data in articles:
        print(f"Source: {source_data['source']}")
        for url in source_data['urls']:
            print(f"URL: {url}")

# Data loading task
def load():
    # Example data loading: Save data to a JSON file
    with open('/path/to/output/articles.json', 'w', encoding='utf-8') as file:
        json.dump(articles, file, ensure_ascii=False, indent=4)
    print("Articles have been saved to articles.json")

# Default arguments for Airflow DAG
default_args = {
    'owner': 'airflow-demo',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 5, 8),
}

# Create Airflow DAG
dag = DAG(
    'mlops-dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='@daily',
    catchup=False
)

# Define Python operators for the DAG tasks
task1 = PythonOperator(
    task_id="extract_data",
    python_callable=extract,
    dag=dag
)

task2 = PythonOperator(
    task_id="transform_data",
    python_callable=transform,
    dag=dag
)

task3 = PythonOperator(
    task_id="load_data",
    python_callable=load,
    dag=dag
)

# Set task dependencies
task1 >> task2 >> task3
