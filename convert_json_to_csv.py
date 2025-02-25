import json
import csv
import sys


def convert_json_to_csv(json_filename, csv_filename):
    # Open and load the JSON file
    with open(json_filename, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    # Extract the rows from the JSON
    rows = data.get("rows", [])

    # Write the rows to a CSV file
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        for row in rows:
            writer.writerow(row)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert_json_to_csv.py input.json output.csv")
        sys.exit(1)

    input_json = sys.argv[1]
    output_csv = sys.argv[2]
    convert_json_to_csv(input_json, output_csv)
