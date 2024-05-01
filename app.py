import  requests
from dotenv import load_dotenv
import os
import json
import csv
import polars as pl
import re

load_dotenv()
username = os.getenv("USER_NAME")
password = os.getenv("PASSWORD")


def get_token(username, password):
    url = "https://api.tequ.fi/api/v1/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "grant_type": "password",
        "username": username,
        "password": password
    }
    response = requests.post(url, headers=headers, data=data)
    return response.json()["access_token"]


def get_data_sources(access_token):
    url = "https://api.tequ.fi/api/v1/datasource"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer " + access_token
    }
    response = requests.get(url, headers=headers)
    return response.json()


def get_data_source(source_id, access_token):
    url = f"https://api.tequ.fi/api/v1/datasource/id/{source_id}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Bearer " + access_token
    }
    response = requests.get(url, headers=headers)
    return response.json()


def get_sensor_data(source_id, sensor_id, access_token):
    url = (f"https://api.tequ.fi/api/v1/sensordata/datasource/{source_id}/sensor/{sensor_id}"
           f"?start_time=2024-01-01T00%3A00%3A00&end_time=2024-02-01T00%3A00%3A00"
           f"&format=plotly&timezone=Europe%2FHelsinki&aggregation=none")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Bearer " + access_token,
        "Accept-Encoding": "gzip"  # Request uncompressed content
    }
    response = requests.get(url, headers=headers)

    # Check if the response is JSON or gzip-encoded
    if response.headers.get('Content-Type') == 'application/json':
        return response.json()
    else:
        return response.content.decode('utf-8')


# Write data to CSV file
def write_to_file(filename, fieldnames, data):
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            # Filter out any keys in the row that are not present in fieldnames
            filtered_row = {key: row[key] for key in fieldnames if key in row}
            writer.writerow(filtered_row)


def is_valid_json(data):
    # Regular expression pattern to match JSON object
    json_pattern = r'^\s*\{.*\}\s*$'

    # Check if the data matches the JSON pattern
    if re.match(json_pattern, data):
        return True
    else:
        return False


#token = get_token(username, password)
token = os.getenv("USER_TOKEN")
data_sources = get_data_sources(token)

sources_list = []
sensors_list = []
sensor_data_list = []
for key in data_sources:
    if key == 's-e05a1b33a0f0':
        continue
    source = get_data_source(key, token)
    sources_list.append(source)
    sensors = source["sensors"]
    print("##################################### SENSORS:")
    print(sensors)
    for sensor in sensors:
        print(source['id'])
        print(sensor['id'])
        sensors_list.append(sensor)
        sensor_data = get_sensor_data(source['id'], sensor['id'], token)

        if is_valid_json(sensor_data):
            data_dict = json.loads(sensor_data)
            if 'x' in data_dict and isinstance(data_dict['x'], list):
                print('found data for sensor')
                # Parse the JSON string into a dictionary
                sensor_data_dict = json.loads(sensor_data)

                # Create a DataFrame using Polars
                df = pl.DataFrame(sensor_data_dict)

                # Display the DataFrame and the label
                print(df)

                # Split the string by parentheses and get the information of the sensor
                label = df['label'][0]
                info = label.split('(')[-1].split(')')[0].replace('/', '_')
                print(info)

                # Save to CSV using Polars
                os.makedirs('./sensor_data_parquet', exist_ok=True)
                parquet_file_path = f'./sensor_data_parquet/{info}.parquet'
                df.write_parquet(parquet_file_path, compression='snappy', row_group_size=100000, use_pyarrow=False, use_threads=True, use_null_rle=True)

                # Save to CSV using pandas
                #df_pandas = df.to_pandas()

                #os.makedirs('./sensor_data', exist_ok=True)
                #csv_file_path = f'./sensor_data/{info}.csv'
                #df_pandas.to_csv(csv_file_path, index=False)

    #sensors_fieldnames = list(sensors_list[0].keys())

    # Write data to CSV files
    #sensors_filename = "sensors_" + source['id'] + ".csv"
    #os.makedirs("./sensors", exist_ok=True)
    #write_to_file(f'./sensors/{sensors_filename}', sensors_fieldnames, sensors_list)

# Write data to CSV files
#sources_fieldnames = list(sources_list[0].keys())
#sources_filename = "sources.csv"
#os.makedirs("./sources", exist_ok=True)
#write_to_file(f'./sources/{sources_filename}', sources_fieldnames, sources_list)
