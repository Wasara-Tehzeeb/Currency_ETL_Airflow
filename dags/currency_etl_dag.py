from datetime import datetime, timedelta, timezone

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
from pymongo import MongoClient, UpdateOne


def extract_currency_rates(**context):
    url = "https://api.frankfurter.dev/v2/rates"

    params = {
        "base": "EUR",
        "quotes": "USD,GBP,JPY"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list) or not data:
        raise ValueError("Unexpected or empty Frankfurter API response")

    context["ti"].xcom_push(
        key="extracted_rates",
        value=data
    )

    print(f"Successfully extracted {len(data)} exchange rates")


def transform_currency_rates(**context):
    ti = context["ti"]

    extracted_data = ti.xcom_pull(
        task_ids="extract_currency_rates",
        key="extracted_rates"
    )

    if not extracted_data:
        raise ValueError("No exchange rates received from Extract task")

    transformed_data = []

    for record in extracted_data:
        transformed_record = {
            "base_currency": record["base"],
            "target_currency": record["quote"],
            "exchange_rate": float(record["rate"]),
            "rate_date": record["date"],
            "source": "Frankfurter API",
            "retrieved_at": datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            )
        }

        transformed_data.append(transformed_record)

    ti.xcom_push(
        key="transformed_rates",
        value=transformed_data
    )

    print(f"Successfully transformed {len(transformed_data)} records")


def load_currency_rates(**context):
    ti = context["ti"]

    transformed_data = ti.xcom_pull(
        task_ids="transform_currency_rates",
        key="transformed_rates"
    )

    if not transformed_data:
        raise ValueError("No transformed data received")

    mongo_uri = os.environ["CURRENCY_MONGO_URI"]

    client = MongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5000
    )

    try:
        collection = client["currency_etl_db"]["exchange_rates"]

        operations = [
            UpdateOne(
                {
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"]
                },
                {"$set": record},
                upsert=True
            )
            for record in transformed_data
        ]

        result = collection.bulk_write(operations)

        print(
            f"MongoDB load completed: "
            f"{result.upserted_count} inserted, "
            f"{result.modified_count} updated, "
            f"{result.matched_count} matched"
        )

    finally:
        client.close()


with DAG(
    dag_id="currency_exchange_etl",
    description="Daily currency exchange rate ETL pipeline",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ETL", "Currency", "MongoDB"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates,
    )
    
    transform_task = PythonOperator(
        task_id="transform_currency_rates",
        python_callable=transform_currency_rates,
    )

    load_task = PythonOperator(
        task_id="load_currency_rates",
        python_callable=load_currency_rates,
    )

    extract_task >> transform_task >> load_task

    