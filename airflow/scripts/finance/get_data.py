import requests
import csv
import math
from google.cloud import storage
import pandas as pd

url = "https://yahoo-finance15.p.rapidapi.com/api/yahoo/co/collections/most_actives"

params = {"start":"0"}

headers = {
	"x-rapidapi-key": "5f737656d9msh0b542b5446e939ep1cd1ebjsn07138dbcbd2a",
	"x-rapidapi-host": "yahoo-finance15.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=params)


if response.status_code == 200:
    data = response.json().get('body', []) 
    csv_filename = 'data_most_actives.csv'

    if data:

        field_names = list(data[0].keys())

        with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=field_names)
            writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print("true")
        
        bucket_name = 'bkt-finance'
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)
    else:
      print("false")

else:
    print(response.status_code)
    