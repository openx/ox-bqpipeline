# ox_bqpipeline.bqpipeline

## get_logger
```python
get_logger(name, fmt='%(asctime)-15s %(levelname)s %(message)s')
```

Creates a Logger that logs to stdout

:param name: name of the logger
:param fmt: format string for log messages
:return: Logger

## read_sql
```python
read_sql(path)
```

Reads UTF-8 encoded SQL from a file
:param path: path to SQL file
:return: str contents of file

## tableref
```python
tableref(project, dataset_id, table_id)
```

Creates a TableReference from project, dataset and table
:param project: project id containing the dataset
:param dataset_id: dataset id
:param table_id: table_id
:return: bigquery.table.TableReference

## to_tableref
```python
to_tableref(tablespec_str)
```

Creates a TableReference from TableSpec
:param tablespec_str: BigQuery TableSpec in format 'project.dataset_id.table_id'
:return: bigquery.table.TableReference

## create_copy_job_config
```python
create_copy_job_config(overwrite=True)
```

Creates CopyJobConfig
:param overwrite: if set to False, target table must not exist
:return: bigquery.job.CopyJobConfig

## exception_logger
```python
exception_logger(func)
```

A decorator that wraps the passed in function and logs
exceptions should one occur

## gcs_export_job_poller
```python
gcs_export_job_poller(func)
```

A decorator to wait on export job

## BQPipeline
```python
BQPipeline(self, job_name, query_project=None, location='US', default_project=None, default_dataset=None, json_credentials_path=None)
```

BigQuery Python SDK Client Wrapper
Provides methods for running queries, copying and deleting tables.
Supports Jinja2 templated SQL and enables default project and dataset to
be set for an entire pipeline.

### get_client
```python
BQPipeline.get_client(self)
```

Initializes bigquery.Client
:return bigquery.Client

### infer_project
```python
BQPipeline.infer_project(self)
```

Infers project based on client's credentials.

### resolve_table_spec
```python
BQPipeline.resolve_table_spec(self, dest)
```

Resolves a full TableSpec from a partial TableSpec by adding default
project and dataset.
:param dest: TableSpec string or partial TableSpec string
:return str TableSpec

### resolve_dataset_spec
```python
BQPipeline.resolve_dataset_spec(self, dataset)
```

Resolves a full DatasetSpec from a partial DatasetSpec by adding default
project.
:param dest: DatasetSpec string or partial DatasetSpec string
:return str DatasetSpec

### create_dataset
```python
BQPipeline.create_dataset(self, dataset, exists_ok=False)
```

Creates a BigQuery Dataset from a full or partial dataset spec.
:param dataset: DatasetSpec string or partial DatasetSpec string

### create_job_config
```python
BQPipeline.create_job_config(self, batch=False, dest=None, create=True, overwrite=True, append=False)
```

Creates a QueryJobConfig
:param batch: use QueryPriority.BATCH if true
:param dest: tablespec of destination table, or a GCS wildcard to
    write query results to.
:param create: if False, destination table must already exist
:param overwrite: if False, destination table must not exist
:param append: if True, destination table will be appended to
:return: bigquery.QueryJobConfig

### run_query
```python
BQPipeline.run_query(self, path, batch=False, wait=True, create=True, overwrite=True, timeout=None, gcs_export_format='CSV', **kwargs)
```

Executes a SQL query from a Jinja2 template file
:param path: path to sql file or tuple of (path to sql file, destination tablespec)
:param batch: run query with batch priority
:param wait: wait for job to complete before returning
:param create: if False, destination table must already exist
:param overwrite: if False, destination table must not exist
:param timeout: time in seconds to wait for job to complete
:param gcs_export_format: CSV, AVRO, or JSON.
:param kwargs: replacements for Jinja2 template
:return: bigquery.job.QueryJob

### run_queries
```python
BQPipeline.run_queries(self, query_paths, batch=True, wait=True, create=True, overwrite=True, timeout=1200, **kwargs)
```

:param query_paths: List[Union[str,Tuple[str,str]]] path to sql file or
       tuple of (path, destination tablespec)
:param batch: run query with batch priority
:param wait: wait for job to complete before returning
:param create: if False, destination table must already exist
:param overwrite: if False, destination table must not exist
:param timeout: time in seconds to wait for job to complete
:param kwargs: replacements for Jinja2 template

### copy_table
```python
BQPipeline.copy_table(self, src, dest, wait=True, overwrite=True, timeout=None)
```

:param src: tablespec 'project.dataset.table'
:param dest: tablespec 'project.dataset.table'
:param wait: block until job completes
:param overwrite: overwrite destination table
:param timeout: time in seconds to wait for operation to complete
:return: bigquery.job.CopyJob

### delete_table
```python
BQPipeline.delete_table(self, table)
```

Deletes a table
:param table: table spec `project.dataset.table`

### delete_tables
```python
BQPipeline.delete_tables(self, tables)
```

Deletes multiple tables
:param tables: List[str] of table spec `project.dataset.table`

### export_csv_to_gcs
```python
BQPipeline.export_csv_to_gcs(self, table, gcs_path, delimiter=',', header=True, wait=True, timeout=None)
```

Export a table to GCS as CSV.
:param table: str of table spec `project.dataset.table`
:param gcs_path: str of destination GCS path
:param delimiter: str field delimiter for output data.
:param header: boolean indicates the output CSV file print the header.

### export_json_to_gcs
```python
BQPipeline.export_json_to_gcs(self, table, gcs_path, wait=True, timeout=None)
```

Export a table to GCS as a Newline Delimited JSON file.
:param table: str of table spec `project.dataset.table`
:param gcs_path: str of destination GCS path

### export_avro_to_gcs
```python
BQPipeline.export_avro_to_gcs(self, table, gcs_path, compression='snappy', wait=True, timeout=None)
```

Export a table to GCS as a Newline Delimited JSON file.
:param table: str of table spec `project.dataset.table`
:param gcs_path: str of destination GCS path

## main
```python
main()
```

Handles CLI invocations of bqpipelines.

