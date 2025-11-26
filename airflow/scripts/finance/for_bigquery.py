from google.cloud import bigquery

client = bigquery.Client()

table_id = "global-calling-478611-m1.finances.data_finances_raw"

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True
)

uri = "gs://bkt-finance/change_data_most_actives.csv"
load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
load_job.result()
destination_table = client.get_table(table_id)
