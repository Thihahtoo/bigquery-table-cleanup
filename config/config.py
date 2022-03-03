project_id = "skyuk-uk-viewing-tf-dev"
region = "EU"
clean_all_tables = False
clean_csv_when_done = False
clean_empty_datasets = True

# table filters
last_modified_date = None      # YYYY-MM-DD. eg: 2019-04-01 or None
min_size_bytes = None               # bytes comsumed by tables or None
object_type = [3]                     # native = 1, view = 2, external = 3. eg: [1,3] or [2] or None
