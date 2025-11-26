import pandas as pd
from google.cloud import storage
import csv

df = pd.read_csv("/opt/airflow/scripts/finance/data_most_actives.csv")

columns_to_drop = ["regularMarketDayRange", "fiftyTwoWeekRange", "corporateActions"]
df.drop(columns=columns_to_drop, inplace=True)
df.to_csv("change_data_most_actives.csv", index=False)

bucket_name = 'bkt-finance'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
destination_blob_name = "change_data_most_actives.csv"

blob = bucket.blob(destination_blob_name)
blob.upload_from_filename("change_data_most_actives.csv")
