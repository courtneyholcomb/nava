import csv
import json
import os

import requests

REQUEST_URL = os.environ["REQUEST_URL"]
DATA_DIR = "data"


def make_requests(schema_filepath, data_filepath):
    columns = get_columns(schema_filepath)

    with open(data_filepath) as data_file:
        # for line in data_file
        row_string = data_file.readline().strip("\n")

        while row_string:
            row = get_json_row(row_string, columns)

            for attempt in range(10):
                resp = requests.post(REQUEST_URL, data=row)
                # log responses
                print(resp)

                if resp.status_code == 201:
                    break
            
            # remove this
            row_string = data_file.readline().strip("\n")


def get_columns(schema_filepath):
    with open(schema_filepath) as schema_file:
        schema_reader = csv.reader(schema_file)
        columns = [column_metadata for column_metadata in schema_reader]

        return columns


def get_string_value(row_string, value_start, column_width):
    value_end = value_start + int(column_width)

    end_if_not_max_length = row_string[value_start:value_end].lstrip().find(" ") + 1

    if end_if_not_max_length:
        value_end = end_if_not_max_length + value_start

    string_value = row_string[value_start:value_end].rstrip()

    return string_value, value_end


def get_json_row(row_string, columns):
    row = {}
    value_start = 0

    for column in columns:
        column_name, column_width, column_datatype = column

        value, value_end = get_string_value(row_string, value_start, column_width)
        row[column_name] = format_value(value, column_datatype)

        value_start = value_end

    return json.dumps(row)


def format_value(value, column_datatype):
    if column_datatype == "TEXT":
        return value

    if column_datatype == "INTEGER":
        return int(value)

    return value == "1"


if __name__ == "__main__":
    # TODO: handle case when data file has no matching schema file
    # get absolute path instead of assuming relative
    data_files = os.listdir(DATA_DIR)

    for data_file in data_files:
        # os.path.join
        data_filepath = "data/" + data_file
        schema_filepath = "schemas/" + data_file[:-3] + "csv"

        make_requests(schema_filepath, data_filepath)
