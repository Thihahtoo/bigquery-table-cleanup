from google.cloud import bigquery
import os

def retrieve_dataset(project_id, region):
     client = bigquery.Client(project = project_id)
     query = f"SELECT * FROM `{project_id}.region-{region}`.INFORMATION_SCHEMA.SCHEMATA"
     print(f"Retrieving datasets from '{project_id}'\n")
     query_job = client.query(query)
     result = query_job.result().to_dataframe()
     return result

def retrieve_table_to_csv(project_id, region, filename, where_clause):
     client = bigquery.Client(project = project_id)
     datasets = retrieve_dataset(project_id, region)
     print(f"Reading tables with where clause '{where_clause}'.\n")
     filename = f"table_list/{filename}.csv"
     if os.path.exists(filename):
          print(f"Found existing '{filename}'")
          os.remove(filename)
          print(f"Delete existing '{filename}'")
     num_table = 0
     num_dataset = 0
     for dataset in datasets.schema_name:
          print(f"Reading '{project_id}.{dataset}'.")
          query = f"""SELECT * FROM (
                    SELECT project_id, dataset_id, table_id, creation_time, last_modified_time, row_count, size_bytes, type, 
                    DATETIME(TIMESTAMP_MILLIS(last_modified_time)) as last_modified_datetime 
                    FROM `{project_id}.{dataset}.__TABLES__`
                    ) {where_clause}"""
          query_job = client.query(query)
          result = query_job.result()
          num_dataset = num_dataset + 1
          num_table = num_table + result.total_rows
          result = result.to_dataframe()
          if num_dataset == 1:
               result.to_csv(filename, index = False, mode='a', header=True, encoding='utf-8')
          else:
               result.to_csv(filename, index = False, mode='a', header=False, encoding='utf-8')
     print(f"Read {num_dataset} datasets.")
     print(f"Generated {num_table} tables.")