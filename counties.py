import pandas as pd
import json

# Load the JSON file
with open('counties.json', 'r') as file:
    data = json.load(file)

# Create a DataFrame from the organisationUnits list
df = pd.DataFrame(data['organisationUnits'])

# Rename 'id' to 'uuid'
df.rename(columns={'id': 'uuid'}, inplace=True)

# Drop the 'level' column
df.drop(columns=['level'], inplace=True)

# Add an incremental id column (starting at 1)
df['incremental_id'] = range(1, len(df) + 1)

# Optionally, reorder the columns if desired
df = df[['incremental_id', 'uuid', 'name']]

# Display the resulting DataFrame
print(df)
