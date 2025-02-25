
# JSON to PostgreSQL Pipeline

This repository contains an Airflow DAG that transforms JSON consumption data and loads it directly into a PostgreSQL database. The DAG processes data from a JSON file (`rdt_kenya_consumption.json`), filters records for last month, groups data by `orgunit_id` (aggregating consumption), and pushes the results to a PostgreSQL table named `consumption_data`.

## Repository Setup

Clone the repository and change to the project directory:

```bash
git clone https://github.com/waitambatha/JSON-to-PostgreSQL-Pipeline
cd JSON-to-PostgreSQL-Pipeline
```

## Pipeline Flow

Below is a flow diagram illustrating the steps of the pipeline. This diagram uses Mermaid syntax, which you can preview in supported editors or online tools.

```mermaid
flowchart TD
    A[Airflow DAG Trigger] --> B[Check PostgreSQL Connection]
    B --> C[Load JSON Data from rdt_kenya_consumption.json]
    C --> D[Compute Last Month (YYYYMM)]
    D --> E[Filter Data for Last Month]
    E --> F[Convert total_consumption to Numeric]
    F --> G[Group by orgunit_id]
    G --> H[Aggregate total_consumption (sum) & average_consumption (mean)]
    H --> I[Push Aggregated Data to PostgreSQL]
```

## Prerequisites

- **Docker & Docker Compose:** Ensure Docker and Docker Compose are installed on your system.
- **Airflow & PostgreSQL Containers:** This project assumes that Airflow is running on Docker Compose and that PostgreSQL is accessible at:
  ```
  postgresql://postgres:masterclass@host.docker.internal:5432/rdt_data
  ```
- **Python Dependencies:** The DAG uses packages like `pandas`, `sqlalchemy`, and `psycopg2`. These are included in the Airflow Docker image or can be installed in your environment.

## Setup Instructions

1. **Place the JSON File**

   Ensure that the file `rdt_kenya_consumption.json` is located in the `dags/` directory of your project.

2. **Configure Airflow & PostgreSQL**

   Update the PostgreSQL connection string in the DAG file (`dags/your_dag_file.py`) if necessary. The DAG is set to connect to PostgreSQL using:
   ```
   postgresql://postgres:masterclass@host.docker.internal:5432/rdt_data
   ```
   Ensure that your Docker Compose configuration allows Airflow to communicate with PostgreSQL (using `host.docker.internal` for example).

3. **Start Airflow**

   Launch your Docker containers using Docker Compose:

   ```bash
   docker-compose up -d
   ```

4. **Trigger the DAG**

   Open the Airflow web UI (usually available at `http://localhost:8080`), locate the DAG named `json_to_postgres_pipeline`, and trigger it manually or wait for the scheduled run.

## DAG Overview

The DAG consists of the following tasks:

1. **check_connection**  
   - Verifies that a connection to the PostgreSQL instance can be established.

2. **transform_and_push**  
   - **Load JSON Data:** Reads data from `rdt_kenya_consumption.json`.
   - **Compute Last Month:** Determines the previous month in `YYYYMM` format.
   - **Filter Data:** Filters rows to only include those matching last month's period.
   - **Data Conversion:** Converts `total_consumption` to a numeric type.
   - **Aggregation:** Groups the data by `orgunit_id`, summing the `total_consumption` and calculating the average consumption (rounded to remove decimals).
   - **Push to PostgreSQL:** Loads the aggregated DataFrame directly into the PostgreSQL table `consumption_data`.

## Troubleshooting

- **No Data in PostgreSQL:**  
  If no data appears in PostgreSQL, verify that the JSON file contains records for the computed last month period (e.g., `"202402"` for February 2024). You can inspect the logs for the transformation task for debugging messages.
  
- **Connection Issues:**  
  Ensure the PostgreSQL connection string is correct and that the Docker networking is configured properly.

## Conclusion

This pipeline demonstrates how to use Airflow (running on Docker) to transform JSON data and load it into PostgreSQL without generating intermediate CSV files. Modify and extend this pipeline to suit your data processing needs.

---

