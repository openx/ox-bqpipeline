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

import datetime
import mock
import unittest
import socket

from google.cloud import bigquery

from ox_bqpipeline import bqpipeline


def mock_gcs_export():
    pass


class TestQueryParameters(unittest.TestCase):
    def setUp(self):
        pass

    def test_table_spec_resolution(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject', default_dataset='testdataset')
        # resolve from table only
        self.assertEqual(bq.resolve_table_spec('testtable'),
                         'testproject.testdataset.testtable')
        # resolve from dataset.table
        self.assertEqual(bq.resolve_table_spec(
            'testdataset.testtable'), 'testproject.testdataset.testtable')
        # leave project.dataset.table unchanged
        self.assertEqual(bq.resolve_table_spec(
            'testproject.testdataset.testtable'), 'testproject.testdataset.testtable')

    def test_create_job_config_default(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject', default_dataset='testdataset')

        cfg = bq.create_job_config()
        self.assertIsNone(cfg.destination)
        self.assertIsNotNone(cfg.default_dataset)
        self.assertEqual(cfg.default_dataset.project, 'testproject')
        self.assertEqual(cfg.default_dataset.dataset_id, 'testdataset')
        self.assertEqual(cfg.create_disposition,
                         bigquery.job.CreateDisposition.CREATE_IF_NEEDED)
        self.assertEqual(cfg.write_disposition,
                         bigquery.job.WriteDisposition.WRITE_TRUNCATE)
        self.assertEqual(cfg.priority, bigquery.QueryPriority.INTERACTIVE)

    def test_create_job_config_destination(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject', default_dataset='testdataset')

        cfgs = [
            bq.create_job_config(dest='testtable'),
            bq.create_job_config(dest='testdataset.testtable'),
            bq.create_job_config(dest='testproject.testdataset.testtable')
        ]

        for cfg in cfgs:
            self.assertEqual(cfg.destination.table_id, 'testtable')
            self.assertEqual(cfg.destination.dataset_id, 'testdataset')
            self.assertEqual(cfg.destination.project, 'testproject')

    def test_create_job_config_flags(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        cfg = bq.create_job_config(batch=False, create=False, overwrite=False)
        self.assertEqual(cfg.priority, bigquery.QueryPriority.INTERACTIVE)
        self.assertEqual(cfg.create_disposition,
                         bigquery.job.CreateDisposition.CREATE_NEVER)
        self.assertEqual(cfg.write_disposition,
                         bigquery.job.WriteDisposition.WRITE_EMPTY)

    def test_scalar_parameters(self):
        result = bqpipeline.set_scalar_parameter('test', 'abc', typ='str')
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'STRING', 'abc'))

        result = bqpipeline.set_scalar_parameter('test', 1, typ='int')
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'INT64', 1))

        dt = datetime.datetime.now()
        result = bqpipeline.set_scalar_parameter('test', dt, typ='datetime')
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'TIMESTAMP', dt))

        b = bytes(123)
        result = bqpipeline.set_scalar_parameter('test', b, typ='bytes')
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'BYTES', b))

        with self.assertRaises(KeyError):
            bqpipeline.set_scalar_parameter('test', 1, typ='unsupported')

    def test_array_parameters(self):
        result = bqpipeline.set_list_parameter('test', ['abc'], typ='list')
        self.assertEqual(result, bigquery.ArrayQueryParameter('test', 'STRING',
                                                              ['abc']))

        result = bqpipeline.set_list_parameter('test', [1, 2], typ='list')
        self.assertEqual(result, bigquery.ArrayQueryParameter('test', 'INT64',
                                                              [1, 2]))

        with self.assertRaises(IndexError):
            bqpipeline.set_list_parameter('test', [], typ='list')

    def test_struct_parameters(self):
        result = bqpipeline.set_dict_parameter('test', {'a': 'abc'},
                                               typ='list')
        self.assertEqual(result, bigquery.StructQueryParameter(
            'test',
            bigquery.ScalarQueryParameter('a', 'STRING', 'abc')))

    def test_positional_parameters(self):
        result = bqpipeline.set_scalar_parameter(None, 'abc', typ='str')
        self.assertEqual(result, bigquery.ScalarQueryParameter(None,
                                                               'STRING', 'abc'))

        result = bqpipeline.set_scalar_parameter(None, 1, typ='int')
        self.assertEqual(result, bigquery.ScalarQueryParameter(None,
                                                               'INT64', 1))

        result = bqpipeline.set_list_parameter(None, ['abc'], typ='list')
        self.assertEqual(result, bigquery.ArrayQueryParameter(None, 'STRING',
                                                              ['abc']))

    def test_query_params(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        cfg = bq.create_job_config(batch=False, create=False, overwrite=False)
        self.assertIsNone(bq.set_query_params([]))

        # Test positional query parameters.
        self.assertEqual(bq.set_query_params([1,]),
                         [bigquery.ScalarQueryParameter(None, 'INT64', 1)])

        # Test named query parameters.
        self.assertEqual(bq.set_query_params({'test': 1}),
                         [bigquery.ScalarQueryParameter('test', 'INT64', 1)])

        # Test invalid query parameters.
        self.assertIsNone(bq.set_query_params([1, [1, 'two']]))

    def test_invalid_query_params(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        cfg = bq.create_job_config(batch=False, create=False, overwrite=False)
        # Test empty query parameters.
        self.assertFalse(bq.validate_query_params(None))

        self.assertFalse(bq.validate_query_params({1: 1}))

    def test_valid_query_params(self):
        bq = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        cfg = bq.create_job_config(batch=False, create=False, overwrite=False)
        self.assertTrue(bq.validate_query_params({'1': {}}))
        self.assertTrue(bq.validate_query_params({'1': {1: 1}}))


class TestLogging(unittest.TestCase):
    def test_name_in_log_suffix(self):
        log_suffix = "test-logs-ftw"
        logger = bqpipeline.get_logger(log_suffix)
        cloud_logger = [(h for h in logger.handlers if type(h._name) == str and
                         h._name.endswith(log_suffix + "}"))]
        self.assertEqual(len(cloud_logger), 1)

    def test_hostname_in_log_suffix(self):
        logger = bqpipeline.get_logger("logs")
        cloud_logger = [(h for h in logger.handlers if type(h._name) == str and
                         socket.gethostname() in h._name)]
        self.assertEqual(len(cloud_logger), 1)
