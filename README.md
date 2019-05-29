# BigQuery Pipeline

Utility class for building data pipelines in BigQuery.

Provides methods for query, copy table, delete table and export to GCS.

Supports Jinja2 templated SQL.


## Usage

Create an instance of BQPipeline. By setting `query_project`, `default_project` and `default_dataset`, you can omit project and dataset from table references in your SQL statements.

`default_project` is the project used when a tablespec does not specify a project.
 
`default_dataset` is the dataset used when a tablespec does not specify project or dataset.

Place files containing a single BigQuery Standard SQL statement per file.

Note that if you reference a project with a '-' in the name, you must backtick the tablespec in your SQL: `` `my-project.dataset_id.table_id` ``

### Writing scripts to be easily portable between environments
- Use `{{ project }}` in all your sql queries
- In your replacements dictionary, set `'project'` to the value of `BQPipeline.infer_project()`
this will infer the project from the credentials. This means in your local shell it will use 
`GOOGLE_APPLICATION_DEFAULT` and on the cron box it will use project of the CronBox's Service Account.

```python
#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ox_bqpipeline.bqpipeline import BQPipeline

bq = BQPipeline(job_name='myjob',
                default_dataset='mydataset',
                json_credentials_path='credentials.json')

replacements = {
    'project': bq.infer_project(),
    'dataset': 'mydataset'
}

bq.copy_table('source_table', 'dest_table')

bq.run_queries(['../sql/q1.sql', ('../sql/q2.sql', 'tmp_table_1')], **replacements)

bq.export_csv_to_gcs('tmp_table_2', 'gs://my-bucket/path/to/tmp_table_2-*.csv')

bq.delete_tables(['tmp_table_1', 'tmp_table_2'])
```

Note, that the `run_queries` method provided this utility can alternatively take a list of tuples where the first entry is the sql path, and the second is a destination table. You can see an example of this in [`example_pipeline.py`](/example_pipeline.py).

For detailed documentation about the methods provided by this utility class see [docs.md](docs.md).

### Creating Service Account JSON Credentials

1. Visit the [Service Account Console](https://console.cloud.google.com/iam-admin/serviceaccounts)
2. Select a service account
3. Select "Create Key"
4. Select "JSON"
5. Click "Create" to download the file


## Installation

### Optional: Install in virtualenv

```
python3 -m virtualenv venv
source venv/bin/activate
```

### Install with pip

```
python3 -m pip install -r requirements.txt
```


## Requirements

You'll need to [download Python 3.4 or later](https://www.python.org/downloads/)

[Google Cloud Python Client](https://github.com/googleapis/google-cloud-python)


## Disclaimer

This is not an official Google project.


## References

[Python Example Code](https://github.com/GoogleCloudPlatform/python-docs-samples)
[google-cloud-bigquery](https://pypi.org/project/google-cloud-bigquery/)
[Jinja2](http://jinja.pocoo.org/docs/2.10/)
