from datetime import datetime, timedelta
import psycopg2
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
    description='Transform JSON data for last month, group by orgunit_id, and push to PostgreSQL'
) as dag:

    def check_connection():
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
        import pandas as pd
        import json
        from datetime import datetime
        from sqlalchemy import create_engine

        # Load JSON from a file
        with open("dags/rdt_kenya_consumption.json", "r") as file:
            data = json.load(file)

        # Convert JSON data to a DataFrame (assuming the JSON has a 'rows' key)
        df = pd.DataFrame(data['rows'])
        # Assign column names manually
        df.columns = ["dataelement", "period", "orgunit_id", "total_consumption"]

        # Compute last month in YYYYMM format
        today = datetime.today()
        year = today.year
        month = today.month
        if month == 1:
            last_month_year = year - 1
            last_month = 12
        else:
            last_month_year = year
            last_month = month - 1
        last_month_str = f"{last_month_year}{last_month:02d}"
        print(f"Filtering data for period: {last_month_str}")

        # Filter rows to only include those with period equal to last month
        # df = df[df['period'] == last_month_str]

        # Convert total_consumption to numeric
        df["total_consumption"] = pd.to_numeric(df["total_consumption"])

        # Group by orgunit_id ensuring that all rows with the same orgunit_id are aggregated into one.
        # For each group, take the first dataelement and period, sum the total_consumption,
        # and compute the average consumption.
        grouped_df = df.groupby('orgunit_id', as_index=False).agg(
            dataelement=('dataelement', 'first'),
            period=('period', 'first'),
            total_consumption=('total_consumption', 'sum'),
            average_consumption=('total_consumption', 'mean')
        )

        # Convert all column headers to lowercase
        grouped_df.columns = [col.lower() for col in grouped_df.columns]

        # Remove decimals from average_consumption by rounding and converting to int
        grouped_df['average_consumption'] = grouped_df['average_consumption'].round(0).astype(int)

        grouped_df['period'] = last_month_str
        # Reorder columns: dataelement, period, orgunit_id, total_consumption, average_consumption
        grouped_df = grouped_df[['dataelement', 'period', 'orgunit_id', 'total_consumption', 'average_consumption']]

        print("Aggregated DataFrame:")
        print(grouped_df)

        # Push the resulting DataFrame directly to PostgreSQL
        engine = create_engine('postgresql://postgres:masterclass@host.docker.internal:5432/rdt_data')
        grouped_df.to_sql('consumption_data', engine, if_exists='replace', index=False)
        print("Data successfully pushed to PostgreSQL!")

    task_check_connection = PythonOperator(
        task_id='check_connection',
        python_callable=check_connection
    )

    task_transform_and_push = PythonOperator(
        task_id='transform_and_push',
        python_callable=transform_and_push
    )

    task_check_connection >> task_transform_and_push
