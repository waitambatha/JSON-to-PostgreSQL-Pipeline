# JSON to PostgreSQL Pipeline (Airflow DAG)

This repository contains an Airflow DAG that transforms JSON consumption data and loads it directly into a PostgreSQL database. The DAG processes data from a JSON file (`rdt_kenya_consumption.json`), filters records for last month, groups data by `orgunit_id` (aggregating consumption), and pushes the results to a PostgreSQL table named `consumption_data`.

## Pipeline Flow

```mermaid
flowchart TD
    A[Airflow DAG Trigger] --> B[Check PostgreSQL Connection]
    B --> C[Load JSON Data]
    C --> D[Filter Data for Last Month (YYYYMM)]
    D --> E[Convert total_consumption to Numeric]
    E --> F[Group by orgunit_id]
    F --> G[Aggregate total_consumption (sum) & average_consumption (mean)]
    G --> H[Push Aggregated Data to PostgreSQL]
```

## Prerequisites

- **Docker:** Make sure Docker is installed.
- **Docker Compose:** Airflow is running on Docker Compose.
- **Airflow & PostgreSQL:** Ensure your Airflow Docker Compose configuration includes both Airflow and PostgreSQL services. The DAG expects PostgreSQL to be accessible via:  
  `postgresql://postgres:masterclass@host.docker.internal:5432/rdt_data`

## Setup Instructions

1. **Clone the Repository**

   ```bash
git clone https://github.com/waitambatha/JSON-to-PostgreSQL-Pipeline
   cd JSON-to-PostgreSQL-Pipeline
   ```

2. **Place the JSON File**

   Place the file `rdt_kenya_consumption.json` in the `dags/` folder. This is the file the DAG will process.

3. **Configure Airflow & PostgreSQL**

   Ensure that your Docker Compose configuration is set up correctly and that Airflow can communicate with PostgreSQL. Update the PostgreSQL connection string in the DAG if necessary.

4. **Start Airflow**

   Launch your Docker containers using Docker Compose:

   ```bash
   docker-compose up -d
   ```

5. **Trigger the DAG**

   Open the Airflow web UI, locate the DAG named `json_to_postgres_pipeline`, and trigger it manually (or wait for its scheduled run).

## DAG Overview

The DAG consists of two main tasks:

1. **check_connection**  
   - Verifies connectivity to the PostgreSQL instance.
2. **transform_and_push**  
   - **Load JSON Data:** Reads data from `rdt_kenya_consumption.json`.
   - **Filter Last Month's Data:** Computes the previous month (in YYYYMM format) and filters the JSON rows accordingly.
   - **Data Conversion:** Converts the `total_consumption` field to a numeric type.
   - **Aggregation:** Groups the data by `orgunit_id`, sums the `total_consumption`, and computes the average consumption (rounded to an integer). For each group, the first encountered `dataelement` and `period` are retained.
   - **Push to PostgreSQL:** Loads the aggregated DataFrame directly into the `consumption_data` table in PostgreSQL.

## Troubleshooting

- **Empty Table:**  
  If no data appears in the PostgreSQL table, ensure that your JSON data contains records for the computed last month period.
  
- **Connection Errors:**  
  Verify that the PostgreSQL connection details are correct and that your Docker setup allows Airflow to communicate with PostgreSQL (e.g., using `host.docker.internal`).

## Conclusion

This DAG provides a robust example of how to transform and aggregate JSON data and then load it directly into PostgreSQL using Airflow running on Docker. Modify and extend this pipeline to fit your own data processing and integration requirements.

---

Feel free to adjust the text, paths, and connection details as needed for your environment.
