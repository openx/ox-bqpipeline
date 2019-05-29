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

import codecs
import os
from google.cloud import bigquery
from jinja2.sandbox import SandboxedEnvironment
import logging
import logging.handlers
import sys



def get_logger(name, logging_path='/home/ubuntu/', fmt='%(asctime)-15s %(levelname)s %(message)s'):
    """
    Creates a Logger that logs to stdout
    :param name: name of the logger
    :param fmt: format string for log messages
    :return: Logger
    """
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

    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    log.addHandler(print_handler)
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

class BQPipeline(object):
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
        self.logger = logging.getLogger(job_name)
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

    def create_job_config(self, batch=True, dest=None, create=True,
                          overwrite=True, append=False):
        """
        Creates a QueryJobConfig
        :param batch: use QueryPriority.BATCH if true
        :param dest: tablespec of destination table
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
            dest_tableref = to_tableref(self.resolve_table_spec(dest))
        else:
            dest_tableref = None

        if self.default_project is not None \
            and self.default_dataset is not None:
            ds = bigquery.dataset.DatasetReference(
                project=self.default_project,
                dataset_id=self.default_dataset
            )
        else:
            ds = None

        return bigquery.QueryJobConfig(priority=priority,
                                       default_dataset=ds,
                                       destination=dest_tableref,
                                       create_disposition=create_disp,
                                       write_disposition=write_disp)

    def run_query(self, path, batch=True, wait=True, create=True,
                  overwrite=True, timeout=None, **kwargs):
        """
        Executes a SQL query from a Jinja2 template file
        :param path: path to sql file or tuple of (path to sql file, destination tablespec)
        :param batch: run query with batch priority
        :param wait: wait for job to complete before returning
        :param create: if False, destination table must already exist
        :param overwrite: if False, destination table must not exist
        :param timeout: time in seconds to wait for job to complete
        :param kwargs: replacements for Jinja2 template
        :return: bigquery.job.QueryJob
        """
        dest = None
        sql_path = path
        if type(path) == tuple:
            sql_path = path[0]
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

    def export_csv_to_gcs(self, table, gcs_path, delimiter=',', header=True,
            wait=True):
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

        job = self.get_client().extract_table(src, gcs_path, job_config=extract_job_config)
        self.logger.info('Extracting table `%s` to `%s` as CSV  %s', table, gcs_path, job.job_id)
        if wait:
            job.result(timeout=timeout)  # wait for job to complete
            job = client.get_job(job.job_id)
            self.logger.info('Finished Extract table `%s` to `%s` as CSV  %s', table, gcs_path, job.job_id)
        return job

    def export_json_to_gcs(self, table, gcs_path):
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

        job = self.get_client().extract_table(src, gcs_path, job_config=extract_job_config)
        self.logger.info('Extracting table `%s` to `%s` as JSON  %s', table, gcs_path, job.job_id)
        if wait:
            job.result(timeout=timeout)  # wait for job to complete
            job = client.get_job(job.job_id)
            self.logger.info('Finished Extract table `%s` to `%s` as JSON  %s', table, gcs_path, job.job_id)
        return job

    def export_avro_to_gcs(self, table, gcs_path, compression='snappy'):
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

        job = self.get_client().extract_table(src, gcs_path, job_config=extract_job_config)
        self.logger.info('Extracting table `%s` to `%s` as JSON  %s', table, gcs_path, job.job_id)
        if wait:
            job.result(timeout=timeout)  # wait for job to complete
            job = client.get_job(job.job_id)
            self.logger.info('Finished Extract table `%s` to `%s` as JSON  %s', table, gcs_path, job.job_id)
        return job
