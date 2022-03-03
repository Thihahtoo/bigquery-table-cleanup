from datetime import datetime
import subprocess
from config import config
from dependencies.generate_tables import retrieve_table_to_csv
from dependencies.cleanup import clean_csv_files, cleanup_tables, delete_empty_dataset

project_id = config.project_id
region = config.region

rundate = datetime.now().strftime('%Y%m%d')
csv_filename = f"table_list_{rundate}"

def run_shell_cmd(cmd):
     return subprocess.run(cmd,
                          shell=True,
                          universal_newlines=True,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)

def create_where_clause():
    clause = []
    if config.last_modified_date:
        last_modified_date = f"last_modified_datetime < '{config.last_modified_date}'"
        clause.append(last_modified_date)
    if config.min_size_bytes:
        table_size = f"size_bytes >= {config.min_size_bytes}"
        clause.append(table_size)
    if config.object_type:
        table_type = config.object_type
        type_clause = ",".join([str(i) for i in table_type])
        type_clause = f"type in ({type_clause})"
        clause.append(type_clause)
     
    clause = " AND ".join(clause)
    clause = "WHERE "+clause
    return clause

### print the run date
print(f'Run on : {rundate}')

### print current account
cmd_find_active_account='gcloud config list account --format "value(core.account)"'
res = run_shell_cmd(cmd_find_active_account)
print(f"You are using the GCP account: {res.stdout}")

### read table from entire project and generate a csv file.
print("Preparing to retrieve tables.")
retrieve_table_to_csv(project_id, region, csv_filename, create_where_clause())

### delete tables according to csv file.
if config.clean_all_tables:
    print("Preparing to clean up tables.")
    cleanup_tables()
else:
    print(f"Edit the 'table_list/{csv_filename}.csv' file to exclude some tables from deletion.")
    ans = input("Continue (y/n):")
    while ans not in ['y','n','Y','N']:
        print("Enter the valid answer.")
        ans = input("Continue (y/n):")
    
    if ans in ['y','Y']:
        print("Preparing to clean up tables.")
        cleanup_tables(csv_filename)
    else:
        print('Exit from program successfully.')
        exit()

### delete csv file when finished.
if config.clean_csv_when_done:
    print("Preparing to clean up csv files.")
    clean_csv_files()

### clean empty datasets in entire project
if config.clean_empty_datasets:
    print("Preparing to clean up empty dataset.")
    delete_empty_dataset(project_id, region)