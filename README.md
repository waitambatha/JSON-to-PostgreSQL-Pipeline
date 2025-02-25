JSON to PostgreSQL Pipeline

This repository contains an Apache Airflow DAG that transforms JSON consumption data and loads it directly into a PostgreSQL database, then generates a JSON import file from the stored data. The DAG performs the following steps:

1. **Check Connection:** Verify connectivity to the PostgreSQL database.
2. **Transform and Push:**  
   - Load the JSON file (`rdt_kenya_consumption.json`) from the `dags/` directory.  
   - Compute the last month in `YYYYMM` format.  
   - Convert and aggregate the consumption data by grouping on `orgunit_id` (summing the total consumption and calculating the average consumption).  
   - Overwrite the period field with the computed last month value.  
   - Push the aggregated data into PostgreSQL (table: consumption_data).
3. **Export to JSON:**  
   - Query the PostgreSQL table for the aggregated data.  
   - Generate a JSON file (`amccalculation.json`) with two category option combinations for each record.

Below is a flow diagram illustrating the pipeline.

## Pipeline Flow Diagram

```mermaid
flowchart TD
    A[Airflow DAG Trigger] --> B[Check PostgreSQL Connection]
    B --> C[Load JSON Data from rdt_kenya_consumption.json]
    C --> D[Compute Last Month in YYYYMM]
    D --> E[Filter & Convert total_consumption]
    E --> F[Aggregate Data by orgunit_id]
    F --> G[Push Aggregated Data to PostgreSQL table consumption_data]
    G --> H[Export Data from PostgreSQL to JSON file amccalculation.json]
    H --> I[End DAG]
```

## Repository Setup

Clone the repository and navigate into the project directory:

```bash
git clone https://github.com/waitambatha/JSON-to-PostgreSQL-Pipeline
cd JSON-to-PostgreSQL-Pipeline
```

## Prerequisites

- **Docker & Docker Compose:** Airflow and PostgreSQL run inside Docker containers.
- **Airflow & PostgreSQL Containers:**  
  The DAG expects PostgreSQL to be accessible at:
  ```
  postgresql://postgres:yourpassword@yourhost/yourdatabase
  ```
- **Python Dependencies:** The DAG uses libraries such as `pandas`, `sqlalchemy`, `psycopg2`, and `json`. These should be available in your Airflow Docker image.

## Setup Instructions

1. **Place the JSON File**  
   Ensure the file `rdt_kenya_consumption.json` is located in the `dags/` directory.

2. **Configure PostgreSQL**  
   Verify that the connection string in the DAG matches your PostgreSQL configuration. The current string is:
   ```
   postgresql://postgres:masterclass@host.docker.internal:5432/rdt_data
   ```
   Adjust if necessary.

3. **Start Docker Containers**  
   Launch your Docker Compose environment:
   ```bash
   docker-compose up -d
   ```

4. **Trigger the DAG**  
   Open the Airflow web UI (typically at [http://localhost:8080](http://localhost:8080)), find the DAG named `json_to_postgres_pipeline`, and trigger it manually or wait for the scheduled run.

## DAG Overview

The DAG consists of three main tasks:

- **check_connection**  
  Verifies that a connection to the PostgreSQL instance can be established.

- **transform_and_push**  
  - **Load JSON Data:** Reads from `dags/rdt_kenya_consumption.json`.  
  - **Compute Last Month:** Determines the previous month in `YYYYMM` format.  
  - **Aggregate Data:** Converts `total_consumption` to numeric, groups data by `orgunit_id` (taking the first encountered `dataelement` and `period`), and calculates both the sum (as `total_consumption`) and mean (as `average_consumption`) for each group.  
  - **Push to PostgreSQL:** The aggregated data is written to the table `consumption_data`.

- **export_to_json**  
  - **Query Data:** Fetches records from the `consumption_data` table.
  - **Generate JSON:** Creates a JSON structure with two entries per record (each with a different category option combo) and writes the output to `dags/amccalculation.json`.

## Troubleshooting

- **No Data in PostgreSQL:**  
  If no data appears in the PostgreSQL table, confirm that your JSON file contains records for the computed last month (formatted as `YYYYMM`).

- **Connection Issues:**  
  Verify the PostgreSQL connection string and ensure Docker networking allows Airflow to communicate with PostgreSQL (using `host.docker.internal`).

## Conclusion

This pipeline demonstrates how to use Apache Airflow (running on Docker) to transform JSON data, load it into PostgreSQL, and then export a JSON import file—all without generating intermediate CSV files. Modify or extend the pipeline to suit your data processing needs.
```
