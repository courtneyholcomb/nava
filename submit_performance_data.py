import csv
import json
import os

import requests
import pathlib


REQUEST_URL = os.environ["REQUEST_URL"]
CURRENT_FILE = pathlib.Path(__file__).parent.absolute()
DATA_DIR = "data"
SCHEMAS_DIR = "schemas"
MAX_RETRIES = 10


def submit_performance_data(schema_filepath, data_filepath):
    columns = get_columns(schema_filepath)

    with open(data_filepath) as data_file:
        for line in data_file:
            row_string = line.strip("\n")

            row = get_json_row(row_string, columns)

            for attempt in range(MAX_RETRIES):
                resp = requests.post(REQUEST_URL, data=row)
                # Log responses
                print(resp)

                # Server returns 201 when successfully created
                if resp.status_code == 201:
                    break

            # TODO: log failure if failed after max retries


def get_columns(schema_filepath):
    with open(schema_filepath) as schema_file:
        schema_reader = csv.reader(schema_file)
        columns = [column_metadata for column_metadata in schema_reader]

        return columns


def get_json_row(row_string, columns):
    row = {}
    value_start = 0

    for column in columns:
        column_name, column_width, column_datatype = column

        # TODO: handle cases when row value does not fit column width 
        value_end = value_start + int(column_width)
        value = row_string[value_start:value_end].strip()

        row[column_name] = format_value(value, column_datatype)

        value_start = value_end

    return json.dumps(row)


def format_value(value, column_datatype):
    if column_datatype == "TEXT":
        return value

    if column_datatype == "INTEGER":
        return int(value)

    if column_datatype == "BOOLEAN":
        return value == "1"
    
    # TODO: handle other datatypes


if __name__ == "__main__":
    # TODO: handle case when data file has no matching schema file
    data_files = os.listdir(os.path.join(CURRENT_FILE, DATA_DIR))

    for data_file in data_files:
        schema_file = data_file.split('.')[0] + ".csv"

        data_filepath = os.path.join(CURRENT_FILE, DATA_DIR, data_file)
        schema_filepath = os.path.join(CURRENT_FILE, SCHEMAS_DIR, schema_file)

        submit_performance_data(schema_filepath, data_filepath)
