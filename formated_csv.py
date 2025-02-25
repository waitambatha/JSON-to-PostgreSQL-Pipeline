import pandas as pd
import json
from datetime import datetime

# Load JSON from a file
with open("dags/rdt_kenya_consumption.json", "r") as file:
    data = json.load(file)

# Convert JSON data to a DataFrame (assuming the JSON is a list of records)
df = pd.DataFrame(data['rows'])
# Assign column names manually
df.columns = ["dataelement", "period", "orgunit_id", "total_consumption"]

# Convert total_consumption to numeric
df["total_consumption"] = pd.to_numeric(df["total_consumption"])

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

# Filter rows to only include those with period equal to last month
df_last_month = df[df['period'] == last_month_str]

# Group by orgunit_id and calculate the required metrics
grouped_df = df_last_month.groupby('orgunit_id', as_index=False).agg(
    dataelement=('dataelement', 'first'),
    total_consumption=('total_consumption', 'sum'),
    average_consumption=('total_consumption', 'mean')
)

# Since all data is from last month, set the period column to last_month_str
grouped_df['period'] = last_month_str

# Convert all column headers to lowercase
grouped_df.columns = [col.lower() for col in grouped_df.columns]

# Remove decimals from average_consumption by rounding and converting to int
grouped_df['average_consumption'] = grouped_df['average_consumption'].round(0).astype(int)

# Reorder the columns: dataelement, period, orgunit_id, total_consumption, average_consumption
grouped_df = grouped_df[['dataelement', 'period', 'orgunit_id', 'total_consumption', 'average_consumption']]

# Write the resulting DataFrame to a new CSV file
grouped_df.to_csv('aggregated_final_output.csv', index=False)

# Display the resulting DataFrame
print(grouped_df)
