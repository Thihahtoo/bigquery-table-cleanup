from google.cloud import bigquery
import os
import csv

def delete_table(project_id, dataset_id, table_name):
    client = bigquery.Client(project= project_id)

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    client.delete_table(table_id, not_found_ok=True) 
    print("Deleted table '{}'.".format(table_id))

def delete_dataset(project_id, dataset_id):
    client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_id}"
    client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
    print("Deleted dataset '{}'.".format(dataset_id))

def clean_csv_files():
    file_list = os.listdir("./table_list/")
    for f in file_list:
        os.remove(f"./table_list/{f}")
    print("Deleted all csv files.\n")

def cleanup_tables(filename):
    with open(f"./table_list/{filename}.csv") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            table_id = f"{row['project_id']}.{row['dataset_id']}.{row['table_id']}"
            print(f"{table_id} -> Deleted")
            #delete_table(row['project_id'],row['dataset_id'],row['table_id'])

def delete_empty_dataset(project_id, region):
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{project_id}.region-{region}`.INFORMATION_SCHEMA.SCHEMATA"
    print(f"Retrieving datasets from '{project_id}'\n")
    query_job = client.query(query)
    datasets = query_job.result().to_dataframe()
    is_empty = False
    
    print("Searching empty dataset.")
    for dataset in datasets.schema_name:
        dataset_id = f"{project_id}.{dataset}"
        dataset = client.get_dataset(dataset_id)
        tables = list(client.list_tables(dataset))
        if not tables:
            is_empty = True
            print(f"Found '{dataset_id}' -> Deleted.")
            #delete_dataset(project_id, dataset_id)

    if not is_empty:
        print("No empty datasets found.")