import pandas as pd

# Load the CSV file with the given column names.
df = pd.read_csv('output.csv', header=None,
                 names=['dataelement', 'period', 'orgunit_id', 'total_consumption'])

# Ensure that total_consumption is numeric.
df['total_consumption'] = pd.to_numeric(df['total_consumption'], errors='coerce')

# Group the data by orgunit_id and aggregate.
grouped_df = df.groupby('orgunit_id').agg(
    total_sum=('total_consumption', 'sum'),
    count=('total_consumption', 'count'),
    average_consumption=('total_consumption', 'mean')
).reset_index()

# Display the summary table.
print(grouped_df)
