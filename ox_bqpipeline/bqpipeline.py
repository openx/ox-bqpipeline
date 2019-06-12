#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import codecs
import datetime
import functools
import getpass
import os
import logging
import logging.handlers
import sys

from google.cloud import bigquery
from google.cloud.logging import Client as LoggingClient
from google.cloud.logging.handlers import  CloudLoggingHandler 
from jinja2.sandbox import SandboxedEnvironment


def get_logger(name, fmt='%(asctime)-15s %(levelname)s %(message)s'):
    """
    Creates a Logger that logs to stdout

    :param name: name of the logger
    :param fmt: format string for log messages
    :return: Logger
    """
    logging_path = os.path.expanduser('~')
    print_handler = logging.StreamHandler(sys.stdout)
    print_handler.setLevel(logging.DEBUG)
    print_handler.setFormatter(logging.Formatter(fmt))
    file_handler = logging.handlers.TimedRotatingFileHandler(
        os.path.join(logging_path, 'bq-pipeline-{}.log'.format(name)),
        when='D',
        interval=30
    )

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(fmt))

    logging_client = LoggingClient()
    cloud_handler = CloudLoggingHandler(logging_client, name='bq-analyst-cron')

    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)
    log.addHandler(print_handler)
    log.addHandler(cloud_handler)
    log.addHandler(file_handler)
    return log

def read_sql(path):
    """
    Reads UTF-8 encoded SQL from a file
    :param path: path to SQL file
    :return: str contents of file
    """
    with codecs.open(path, mode='r', encoding='utf-8', buffering=-1) as sql_file:
        return sql_file.read()


def tableref(project, dataset_id, table_id):
    """
    Creates a TableReference from project, dataset and table
    :param project: project id containing the dataset
    :param dataset_id: dataset id
    :param table_id: table_id
    :return: bigquery.table.TableReference
    """
    dataset_ref = bigquery.dataset.DatasetReference(project=project, dataset_id=dataset_id)
    return bigquery.table.TableReference(dataset_ref=dataset_ref, table_id=table_id)


def to_tableref(tablespec_str):
    """
    Creates a TableReference from TableSpec
    :param tablespec_str: BigQuery TableSpec in format 'project.dataset_id.table_id'
    :return: bigquery.table.TableReference
    """
    parts = tablespec_str.split('.')
    return tableref(parts[0], parts[1], parts[2])


def create_copy_job_config(overwrite=True):
    """
    Creates CopyJobConfig
    :param overwrite: if set to False, target table must not exist
    :return: bigquery.job.CopyJobConfig
    """
    if overwrite:
        # Target table will be overwritten
        return bigquery.job.CopyJobConfig(
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE)
    # Target table must not exist
    return bigquery.job.CopyJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_EMPTY)

def exception_logger(func):
    """
    A decorator that wraps the passed in function and logs 
    exceptions should one occur
    """
    logger = logging.getLogger(__name__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            # log the exception
            err = "There was an exception in {}: ".format(func.__name__)
            err += func.__name__
            logger.exception(err)
            raise
    return wrapper

def gcs_export_job_poller(func):
    """
    A decorator to wait on export job
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
       logger = logging.getLogger(__name__)
       job = func(*args, **kwargs)
       job.result(timeout=kwargs.get('timeout'))  # wait for job to complete
       logger.info('Finished Extract to GCS. jobId: %s',
                        job.job_id)
    return wrapper

class BQPipeline():
    """
    BigQuery Python SDK Client Wrapper
    Provides methods for running queries, copying and deleting tables.
    Supports Jinja2 templated SQL and enables default project and dataset to
    be set for an entire pipeline.
    """

    def __init__(self,
                 job_name,
                 query_project=None,
                 location='US',
                 default_project=None,
                 default_dataset=None,
                 json_credentials_path=None):
        """
        :param job_name: used as job name prefix
        :param query_project: project used to submit queries
        :param location: BigQuery defaults to 'US'
        :param default_project: project to use when tablespec does not specify
            project
        :param default_dataset: dataset to use when tablespec does not specify
            dataset, if default_project is also set
        :param json_credentials_path: (optional) path to service account JSON
            credentials file
        """
        self.logger = logging.getLogger(__name__)
        self.job_name = job_name
        self.job_id_prefix = job_name + '-'
        self.location = location
        self.query_project = None # inferred from service account.
        self.default_project = default_project
        self.json_credentials_path = json_credentials_path
        self.default_dataset = default_dataset
        self.bq = None
        self.jinja2 = SandboxedEnvironment()


    def get_client(self):
        """
        Initializes bigquery.Client
        :return bigquery.Client
        """
        if self.bq is None:
            if self.json_credentials_path is not None:
                self.bq = bigquery.Client.from_service_account_json(self.json_credentials_path)
            else:
                self.bq = bigquery.Client(project=self.query_project, location=self.location)

            self.query_project = self.bq.project
            if self.default_project is None:
                self.default_project = self.bq.project
        return self.bq

    def infer_project(self):
        """
        Infers project based on client's credentials.
        """
        return self.get_client().project

    def resolve_table_spec(self, dest):
        """
        Resolves a full TableSpec from a partial TableSpec by adding default
        project and dataset.
        :param dest: TableSpec string or partial TableSpec string
        :return str TableSpec
        """
        if type(dest) == bigquery.table.TableReference:
            return dest
        table_id = dest
        if table_id is not None:
            parts = table_id.split('.')
            if len(parts) == 2 and self.default_project is not None:
                table_id = self.default_project + '.' + dest
            elif len(parts) == 1 and \
                self.default_project is not None \
                and self.default_dataset is not None:
                table_id = self.default_project + '.' + self.default_dataset + '.' + dest
        return table_id

    def resolve_dataset_spec(self, dataset):
        """
        Resolves a full DatasetSpec from a partial DatasetSpec by adding default
        project.
        :param dest: DatasetSpec string or partial DatasetSpec string
        :return str DatasetSpec
        """
        dataset_id = dataset
        if dataset_id is not None:
            parts = dataset_id.split('.')
            if len(parts) == 1 and \
                self.default_project is not None:
                dataset_id = self.default_project + '.' + dataset
        return dataset_id

    @exception_logger
    def create_dataset(self, dataset, exists_ok=False):
        """
        Creates a BigQuery Dataset from a full or partial dataset spec.
        :param dataset: DatasetSpec string or partial DatasetSpec string
        """
        return self.bq.create_dataset(self.resolve_dataset_spec(dataset),
                                      exists_ok=exists_ok)

    def create_job_config(self, batch=False, dest=None, create=True,
                          overwrite=True, append=False):
        """
        Creates a QueryJobConfig
        :param batch: use QueryPriority.BATCH if true
        :param dest: tablespec of destination table, or a GCS wildcard to
            write query results to.
        :param create: if False, destination table must already exist
        :param overwrite: if False, destination table must not exist
        :param append: if True, destination table will be appended to
        :return: bigquery.QueryJobConfig
        """
        if create:
            create_disp = bigquery.job.CreateDisposition.CREATE_IF_NEEDED
        else:
            create_disp = bigquery.job.CreateDisposition.CREATE_NEVER

        if overwrite:
            write_disp = bigquery.job.WriteDisposition.WRITE_TRUNCATE
        elif append:
            write_disp = bigquery.job.WriteDisposition.WRITE_APPEND
        else:
            write_disp = bigquery.job.WriteDisposition.WRITE_EMPTY

        if batch:
            priority = bigquery.QueryPriority.BATCH
        else:
            priority = bigquery.QueryPriority.INTERACTIVE

        if dest is not None:
            # If the destination is a GCS path, don't set a destinaion table.
            if dest.startswith('gs://'):
                dest_tableref = None
            dest_tableref = to_tableref(self.resolve_table_spec(dest))
        else:
            dest_tableref = None

        if self.default_project is not None \
            and self.default_dataset is not None:
            data_set = bigquery.dataset.DatasetReference(
                project=self.default_project,
                dataset_id=self.default_dataset
            )
        else:
            data_set = None

        return bigquery.QueryJobConfig(priority=priority,
                                       default_dataset=data_set,
                                       destination=dest_tableref,
                                       create_disposition=create_disp,
                                       write_disposition=write_disp)

    @exception_logger
    def run_query(self, path, batch=False, wait=True, create=True,
                  overwrite=True, timeout=None, gcs_export_format='CSV', **kwargs):
        """
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
        """
        dest = None
        sql_path = path
        if type(path) == tuple:
            is_gcs_dest = path[1].startswith('gs://')
            sql_path = path[0]
            if not is_gcs_dest:
                dest = self.resolve_table_spec(path[1])

        template_str = read_sql(sql_path)
        template = self.jinja2.from_string(template_str)
        query = template.render(**kwargs)
        client = self.get_client()
        job = client.query(query,
                           job_config=self.create_job_config(batch, dest, create, overwrite),
                           job_id_prefix=self.job_id_prefix)
        self.logger.info('Executing query %s %s', sql_path, job.job_id)
        if wait:
            job.result(timeout=timeout)  # wait for job to complete
            job = client.get_job(job.job_id)
            self.logger.info('Finished query %s %s', sql_path, job.job_id)

        if is_gcs_dest:
            if gcs_export_format == 'CSV':
                self.export_csv_to_gcs(job.destination, path[1], delimiter=',', header=True)
            elif gcs_export_format == 'JSON':
                self.export_json_to_gcs(job.destination, path[1])
            elif gcs_export_format == 'AVRO':
                self.export_avro_to_gcs(job.destination, path[1])

        return job

    def run_queries(self, query_paths, batch=True, wait=True, create=True,
                    overwrite=True, timeout=20*60, **kwargs):
        """
        :param query_paths: List[Union[str,Tuple[str,str]]] path to sql file or
               tuple of (path, destination tablespec)
        :param batch: run query with batch priority
        :param wait: wait for job to complete before returning
        :param create: if False, destination table must already exist
        :param overwrite: if False, destination table must not exist
        :param timeout: time in seconds to wait for job to complete
        :param kwargs: replacements for Jinja2 template
        """
        for path in query_paths:
            self.run_query(path, batch=batch, wait=wait, create=create,
overwrite=overwrite, timeout=timeout, **kwargs)

    @exception_logger
    def copy_table(self, src, dest, wait=True, overwrite=True, timeout=None):
        """
        :param src: tablespec 'project.dataset.table'
        :param dest: tablespec 'project.dataset.table'
        :param wait: block until job completes
        :param overwrite: overwrite destination table
        :param timeout: time in seconds to wait for operation to complete
        :return: bigquery.job.CopyJob
        """
        src = self.resolve_table_spec(src)
        dest = self.resolve_table_spec(dest)
        job = self.get_client().copy_table(sources=src,
                                           destination=dest,
                                           job_id_prefix=self.job_id_prefix,
                                           job_config=create_copy_job_config(overwrite=overwrite))
        self.logger.info('Copying table `%s` to `%s` %s', src, dest, job.job_id)
        if wait:
            job.result(timeout=timeout)  # wait for job to complete
            self.logger.info('Finished copying table `%s` to `%s` %s', src, dest, job.job_id)
            job = self.get_client().get_job(job.job_id)
        return job

    @exception_logger
    def delete_table(self, table):
        """
        Deletes a table
        :param table: table spec `project.dataset.table`
        """
        table = self.resolve_table_spec(table)
        self.logger.info("Deleting table `%s`", table)
        self.get_client().delete_table(table)

    def delete_tables(self, tables):
        """
        Deletes multiple tables
        :param tables: List[str] of table spec `project.dataset.table`
        """
        for table in tables:
            self.delete_table(table)

    @exception_logger
    @gcs_export_job_poller
    def export_csv_to_gcs(self, table, gcs_path, delimiter=',', header=True,
                          wait=True, timeout=None):
        """
        Export a table to GCS as CSV.
        :param table: str of table spec `project.dataset.table`
        :param gcs_path: str of destination GCS path
        :param delimiter: str field delimiter for output data.
        :param header: boolean indicates the output CSV file print the header.
        """
        src = self.resolve_table_spec(table)
        extract_job_config = bigquery.job.ExtractJobConfig(
            compression='NONE',
            destination_format='CSV',
            field_delimiter=delimiter,
            print_header=header
        )

        gcs_path = os.path.join(gcs_path, self.job_name, datetime.datetime.now().strftime("jobRunTime=%Y-%m-%dT%H%M%S"),
                                self.job_name + "-export-*.csv")

        job = self.get_client().extract_table(src, gcs_path, job_config=extract_job_config)
        self.logger.info('Extracting table `%s` to `%s` as CSV  %s', table, gcs_path, job.job_id)
        return job

    @exception_logger
    @gcs_export_job_poller
    def export_json_to_gcs(self, table, gcs_path, wait=True, timeout=None):
        """
        Export a table to GCS as a Newline Delimited JSON file.
        :param table: str of table spec `project.dataset.table`
        :param gcs_path: str of destination GCS path
        """
        src = self.resolve_table_spec(table)
        extract_job_config = bigquery.job.ExtractJobConfig(
            compression='NONE',
            destination_format='NEWLINE_DELIMITED_JSON',
        )

        gcs_path = os.path.join(gcs_path, self.job_name, datetime.datetime.now().strftime("jobRunTime=%Y%m%d%h%m%s"),
                                self.job_name + "-export-*.json")

        job = self.get_client().extract_table(src, gcs_path, job_config=extract_job_config)
        self.logger.info('Extracting table `%s` to `%s` as JSON  %s', table, gcs_path, job.job_id)
        return job

    @exception_logger
    @gcs_export_job_poller
    def export_avro_to_gcs(self, table, gcs_path, compression='snappy',
                           wait=True, timeout=None):
        """
        Export a table to GCS as a Newline Delimited JSON file.
        :param table: str of table spec `project.dataset.table`
        :param gcs_path: str of destination GCS path
        """
        src = self.resolve_table_spec(table)
        extract_job_config = bigquery.job.ExtractJobConfig(
            compression=compression,
            destination_format='AVRO'
        )

        gcs_path = os.path.join(gcs_path, self.job_name, datetime.datetime.now().strftime("jobRunTime=%Y%m%d%h%m%s"),
                                self.job_name + "-export-*.avro")

        job = self.get_client().extract_table(src, gcs_path, job_config=extract_job_config)
        self.logger.info('Extracting table `%s` to `%s` as AVRO  %s', table, gcs_path, job.job_id)
        return job
def main():
    """
    Handles CLI invocations of bqpipelines.
    """
    print_handler = logging.StreamHandler(sys.stdout)
    print_handler.setLevel(logging.DEBUG)
    fmt = '%(asctime)-15s %(levelname)s %(message)s'
    print_handler.setFormatter(logging.Formatter(fmt))

    job_name = getpass.getuser() + '-cli-job'
    log = logging.getLogger(job_name)
    log.setLevel(logging.DEBUG)
    log.addHandler(print_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('--query_file', dest='query_file', required=True,
                        help="Path to your bigquery sql file.")
    parser.add_argument('--gcs_destination', dest='gcs_destination', required=False,
                        help="GCS wildcard path to write files.", default=None)
    parser.add_argument('--gcs_export_format', dest='gcs_format', required=False,
                        help="Format for export. CSV | AVRO | JSON", default='CSV')
    args = parser.parse_args()

    bqp = BQPipeline(job_name)
    bqp.run_query((args.query_file, args.gcs_destination), gcs_format=args.gcs_format)

if __name__ == "__main__":
    main()
