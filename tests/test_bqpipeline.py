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
import socket
import unittest

from google.cloud import bigquery

from ox_bqpipeline import bqpipeline


def mock_gcs_export(cls, table, gcs_path, delimiter=',', header=True,
                    wait=True, timeout=None):
    pass


class TestJobConfig(unittest.TestCase):

    def test_table_spec_resolution(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        # resolve from table only
        self.assertEqual(bqp.resolve_table_spec('testtable'),
                         'testproject.testdataset.testtable')
        # resolve from dataset.table
        self.assertEqual(bqp.resolve_table_spec(
            'testdataset.testtable'), 'testproject.testdataset.testtable')
        # leave project.dataset.table unchanged
        self.assertEqual(bqp.resolve_table_spec(
            'testproject.testdataset.testtable'), 'testproject.testdataset.testtable')

    def test_create_job_config_default(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')

        cfg = bqp.create_job_config()
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
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')

        cfgs = [
            bqp.create_job_config(dest='testtable'),
            bqp.create_job_config(dest='testdataset.testtable'),
            bqp.create_job_config(dest='testproject.testdataset.testtable')
        ]

        for cfg in cfgs:
            self.assertEqual(cfg.destination.table_id, 'testtable')
            self.assertEqual(cfg.destination.dataset_id, 'testdataset')
            self.assertEqual(cfg.destination.project, 'testproject')

    def test_create_job_config_flags(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        cfg = bqp.create_job_config(batch=False, create=False, overwrite=False)
        self.assertEqual(cfg.priority, bigquery.QueryPriority.INTERACTIVE)
        self.assertEqual(cfg.create_disposition,
                         bigquery.job.CreateDisposition.CREATE_NEVER)
        self.assertEqual(cfg.write_disposition,
                         bigquery.job.WriteDisposition.WRITE_EMPTY)

class TestQueryParameters(unittest.TestCase):

    def test_scalar_parameters(self):
        result = bqpipeline.set_parameter('test', 'abc')
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'STRING',
                                                               'abc'))

        result = bqpipeline.set_parameter('test', 1)
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'INT64', 1))

        result = bqpipeline.set_parameter('test', 1.0009)
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'NUMERIC',
                                                               1.0009))

        result = bqpipeline.set_parameter(
            'test', 9999999999999999999999999999999.999999999)
        self.assertEqual(result, bigquery.ScalarQueryParameter(
            'test', 'FLOAT64', 9999999999999999999999999999999.999999999))

        dt = datetime.datetime.now()
        result = bqpipeline.set_parameter('test', dt)
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'TIMESTAMP',
                                                               dt))

        dt = datetime.date.today()
        result = bqpipeline.set_parameter('test', dt)
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'DATE',
                                                               dt))

        b = bytes(123)
        result = bqpipeline.set_parameter('test', b)
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'BYTES', b))

        b = True
        result = bqpipeline.set_parameter('test', b)
        self.assertEqual(result, bigquery.ScalarQueryParameter('test',
                                                               'BOOL', b))

    def test_array_parameters(self):
        result = bqpipeline.set_parameter('test', ['abc'])
        self.assertEqual(result, bigquery.ArrayQueryParameter('test', 'STRING',
                                                              ['abc']))

        result = bqpipeline.set_parameter('test', [1, 2])
        self.assertEqual(result, bigquery.ArrayQueryParameter('test', 'INT64',
                                                              [1, 2]))

        val = [1.0, 9999999999999999999999999999999.999999999]
        result = bqpipeline.set_parameter('test', val)
        self.assertEqual(result, bigquery.ArrayQueryParameter(
            'test',
            'FLOAT64', val))

        result = bqpipeline.set_parameter('test', [1.0, 2.01])
        self.assertEqual(result, bigquery.ArrayQueryParameter('test', 'NUMERIC',
                                                              [1.0, 2.01]))

        with self.assertRaises(ValueError):
            bqpipeline.set_parameter('test', [])

    def test_struct_parameters(self):
        result = bqpipeline.set_parameter('test', {'a': 'abc'})
        self.assertEqual(result, bigquery.StructQueryParameter(
            'test',
            bigquery.ScalarQueryParameter('a', 'STRING', 'abc')))

    def test_positional_parameters(self):
        result = bqpipeline.set_parameter(None, 'abc')
        self.assertEqual(result, bigquery.ScalarQueryParameter(None,
                                                               'STRING', 'abc'))

        result = bqpipeline.set_parameter(None, 1)
        self.assertEqual(result, bigquery.ScalarQueryParameter(None,
                                                               'INT64', 1))

        result = bqpipeline.set_parameter(None, ['abc'])
        self.assertEqual(result, bigquery.ArrayQueryParameter(None, 'STRING',
                                                              ['abc']))

    def test_query_params(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        self.assertIsNone(bqp.set_query_params([]))

        # Test positional query parameters.
        self.assertEqual(bqp.set_query_params([1,]),
                         [bigquery.ScalarQueryParameter(None, 'INT64', 1)])

        # Test named query parameters.
        self.assertEqual(bqp.set_query_params({'test': 1}),
                         [bigquery.ScalarQueryParameter('test', 'INT64', 1)])

        # Test invalid query parameters.
        with self.assertRaises(ValueError):
            bqp.set_query_params([1, [1, 'two']])

    def test_invalid_query_params(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        # Test empty query parameters.
        self.assertFalse(bqp.validate_query_params(None))

        self.assertFalse(bqp.validate_query_params([[]]))

        self.assertFalse(bqp.validate_query_params({'1': []}))

        self.assertFalse(bqp.validate_query_params({1: 1}))

        self.assertFalse(bqp.validate_query_params([1, [1, 'two']]))

        self.assertFalse(bqp.validate_query_params({'1': {}}))

        self.assertFalse(bqp.validate_query_params({'1': {1: 1}}))

        self.assertFalse(bqp.validate_query_params({'1': [{1: 'a', '2': 'b'},
                                                         {1: 'c', '2': 'b'}]}))

    def test_valid_query_params(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='testproject',
            default_dataset='testdataset')
        self.assertTrue(bqp.validate_query_params({'1': {'1': 1}}))

        self.assertTrue(bqp.validate_query_params({'1': {'1': 'a', '2': 2}}))

        self.assertTrue(bqp.validate_query_params({'1': {'1': 'a',
                                                         '2': {'b': 'c'},
                                                         '3': [1, 2, 3]},
                                                   '2': [1, 2, 3]}))

    def test_run_query(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='ox-data-analytics-devint',
            default_dataset='scratch')
        with mock.patch.object(bqpipeline.BQPipeline, 'export_csv_to_gcs',
                               new=mock_gcs_export):
            qj = bqp.run_query(('./tests/sql/select_query1.sql',
                                'gs://mockpath', {'a': 1, 'b': 'one'}),
                               batch=False, overwrite=False,
                               dry_run=False)
            result = [r.values() for r in qj.result()]
            self.assertTrue(qj.done())
            self.assertEqual(result[0], (1, 'one'))

    def test_run_queries(self):
        bqp = bqpipeline.BQPipeline(
            job_name='testjob', default_project='ox-data-analytics-devint',
            default_dataset='scratch')

        with mock.patch.object(bqpipeline.BQPipeline, 'export_csv_to_gcs',
                               new=mock_gcs_export):
            qj_list = bqp.run_queries(
                [('./tests/sql/select_query1.sql', 'gs://mockpath',
                  {'a': 1, 'b': 'one'}),
                 ('./tests/sql/select_query2.sql', 'gs://mockpath',
                  {'e': {'c': 'duos', 'd': 'don'}}),
                 ('./tests/sql/select_query3.sql', 'gs://mockpath'),
                ],
                batch=False, overwrite=False,
                dry_run=False)
            expected_list = [(1, 'one'),
                             (2, 'two', {'c': 'duos', 'd': 'don'}),
                             (1, 'one'),]
            for i, expected in enumerate(expected_list):
                result = [r.values() for r in qj_list[i].result()]
                self.assertEqual(expected, result[0])


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
