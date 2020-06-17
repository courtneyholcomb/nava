import csv
import json
import os
import pathlib

import requests


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

            row = get_row(row_string, columns)
            json_row = json.dumps(row)
            row_id = row["measure_id"]

            for attempt in range(MAX_RETRIES):
                resp = requests.post(REQUEST_URL, data=json_row)
                # Log responses
                print(resp)

                # Server returns 201 when successfully created
                if resp.status_code == 201:
                    break

                elif resp.status_code == 409:
                    # delete existing row
                    resp = requests.delete(REQUEST_URL + "?measure_id=" + row_id)
                    # Log responses
                    print(resp)

                    # if resp code is 400:
                    
            # TODO: log failure if failed after max retries


def get_columns(schema_filepath):
    with open(schema_filepath) as schema_file:
        schema_reader = csv.reader(schema_file)
        columns = [column_metadata for column_metadata in schema_reader]

        return columns


def get_row(row_string, columns):
    row = {}
    value_start = 0

    for column in columns:
        column_name, column_width, column_datatype = column

        # TODO: handle cases when row value does not fit column width
        value_end = value_start + int(column_width)
        value = row_string[value_start:value_end].strip()

        row[column_name] = format_value(value, column_datatype)

        value_start = value_end

    return row


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
    data_files_unordered = os.listdir(os.path.join(CURRENT_FILE, DATA_DIR))

    # TODO today: put this into a helper function

    data_files_dates = []
    for data_file in data_files_unordered:
        if data_file != ".DS_Store":
            data_file_pieces = data_file.split("_")
            data_file_prefix = data_file_pieces[0]
            file_date = data_file_pieces[1].split(".")[0]
            data_files_dates.append((file_date, data_file_prefix, data_file))
    
    data_files_dates.sort()

    for data_file_pieces in data_files_dates:
        file_date, data_file_prefix, data_file = data_file_pieces

        schema_file = data_file_prefix + ".csv"

        data_filepath = os.path.join(CURRENT_FILE, DATA_DIR, data_file)
        schema_filepath = os.path.join(CURRENT_FILE, SCHEMAS_DIR, schema_file)

        submit_performance_data(schema_filepath, data_filepath)
