# BigQuery Pipeline

Utility class for building data pipelines in BigQuery.

Provides methods for query, copy table, delete table and export to GCS.

Supports Jinja2 templated SQL.

# Getting Started
[Check out the codelab!](https://codinginadtech.com/ox-bqpipeline/codelab/index.html#0)

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

```
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

### Writing scripts with parameterized queries

Bigquery standard sql provides support for [parameterized queries](https://cloud.google.com/bigquery/docs/parameterized-queries#top_of_page).

To run parameterized queries using the bqpipeline from commandline, use the following syntax:

```bash
python3 ox_bqpipeline/bqpipeline.py --query_file query.sql --gcs_destination gs://bucket_path --query_params '{"int_param": 1, "str_param": "one"}'
```

In order to invoke the BQPipelines.run_queries method from within your python
module, use the following pattern.

```python
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='project_name',
            default_dataset='dataset_name')
        qj_list = bqp.run_queries(
            [(<query1_path>, <table_or_gcs_dest>, {'query1_params key,val'}),
             (<query2_path>, <table_or_gcs_dest>, {'query2_params key,val'}),
            ],
            batch=False, overwrite=False,
            dry_run=False)
```

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

```bash
pipenv install --python 3
```

### Install with pipenv

```bash
python3 -m pip install -r requirements.txt
```

or

```bash
pip3 install -r requirements.txt
```

### Run test suite

```bash
python3 -m unittest discover
```

### Run test suite using pipenv

```bash
pipenv run python -m unittest discover
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
[ox_bqpipeline Reference](docs.md)
