from datetime import datetime, timedelta
import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'json_to_postgres_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Transform JSON data, store in PostgreSQL, and generate JSON import file'
) as dag:

    def check_connection():
        """Check database connectivity."""
        try:
            conn = psycopg2.connect(
                host="host.docker.internal",
                database="rdt_data",
                user="postgres",
                password="masterclass"
            )
            conn.close()
            print("Connection successful!")
        except Exception as e:
            print(f"Connection failed: {e}")
            raise e

    def transform_and_push():
        """Transform JSON data and push it to PostgreSQL."""
        # Load JSON from file
        with open("dags/rdt_kenya_consumption.json", "r") as file:
            data = json.load(file)

        df = pd.DataFrame(data['rows'])
        df.columns = ["dataelement", "period", "orgunit_id", "total_consumption"]

        # Compute last month
        today = datetime.today()
        last_month = today.month - 1 or 12
        last_year = today.year - (1 if last_month == 12 else 0)
        last_month_str = f"{last_year}{last_month:02d}"
        print(f"Filtering data for period: {last_month_str}")

        # Convert total_consumption to numeric
        df["total_consumption"] = pd.to_numeric(df["total_consumption"])

        # Aggregate data
        grouped_df = df.groupby('orgunit_id', as_index=False).agg(
            dataelement=('dataelement', 'first'),
            period=('period', 'first'),
            total_consumption=('total_consumption', 'sum'),
            average_consumption=('total_consumption', 'mean')
        )

        grouped_df.columns = [col.lower() for col in grouped_df.columns]
        grouped_df['average_consumption'] = grouped_df['average_consumption'].round(0).astype(int)
        grouped_df['period'] = last_month_str

        # Store in PostgreSQL
        engine = create_engine('postgresql://postgres:masterclass@host.docker.internal:5432/rdt_data')
        grouped_df.to_sql('consumption_data', engine, if_exists='replace', index=False)
        print("Data successfully pushed to PostgreSQL!")

    def export_to_json():
        """Fetch records from PostgreSQL and generate JSON import file."""
        conn = psycopg2.connect(
            host="host.docker.internal",
            database="rdt_data",
            user="postgres",
            password="masterclass"
        )
        cursor = conn.cursor()

        # Fetch data
        query = """
        SELECT dataelement, period, orgunit_id, total_consumption, average_consumption
        FROM consumption_data
        """
        cursor.execute(query)
        records = cursor.fetchall()

        # Construct JSON structure
        data = {"dataValues": []}
        optioncombo = 'QvctQfKAQn3'  # Category option 1
        optioncombo2 = 'miM6uIJ2cWx'  # Category option 2

        for record in records:
            dataelement, period, orgunit_id, total_consumption, average_consumption = record

            data["dataValues"].append({
                "dataElement": dataelement,
                "period": period,
                "orgUnit": orgunit_id,
                "categoryOptionCombo": optioncombo,
                "value": average_consumption
            })

            data["dataValues"].append({
                "dataElement": dataelement,
                "period": period,
                "orgUnit": orgunit_id,
                "categoryOptionCombo": optioncombo2,
                "value": average_consumption
            })

        # Save JSON file
        with open('dags/amccalculation.json', 'w') as outfile:
            json.dump(data, outfile, indent=4)

        print("JSON import file successfully generated!")

        cursor.close()
        conn.close()

    task_check_connection = PythonOperator(
        task_id='check_connection',
        python_callable=check_connection
    )

    task_transform_and_push = PythonOperator(
        task_id='transform_and_push',
        python_callable=transform_and_push
    )

    task_export_to_json = PythonOperator(
        task_id='export_to_json',
        python_callable=export_to_json
    )

    task_check_connection >> task_transform_and_push >> task_export_to_json
