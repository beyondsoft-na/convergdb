# ConvergDB - DevOps for Data
# Copyright (C) 2018 Beyondsoft Consulting, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import unittest
import json
import os

import athena_query_reporter

my_path = os.path.abspath(os.path.dirname(__file__))

athena_response_file = os.path.join(
    my_path, "test_athena_batch_get_query_executions.json")

with open(athena_response_file) as athena_response_json:
    athena_response = json.load(athena_response_json)


class TestAthenaQueryReporter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ['QUERY_TRACKING_TABLE'] = 'test'

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('QUERY_TRACKING_TABLE')

    def test_batch_metrics(self):
        metrics = list(range(0, 42))
        batches = athena_query_reporter.batch_metrics(metrics)
        # Should have three batches
        self.assertEqual(len(batches), 3)
        # The first two batches each contain 20 items
        self.assertEqual(len(batches[0]), 20)
        self.assertEqual(len(batches[1]), 20)
        # The third batch contains 2 items.
        self.assertEqual(len(batches[2]), 2)

    def test_executions_to_metrics(self):
        metrics = athena_query_reporter.executions_to_metrics(
            athena_response.get('QueryExecutions'))
        # From ten query executions, it should generate 25 metrics.
        self.assertEqual(len(metrics), 25)
        # There should be 5 'QuerySuccesses' metrics
        self.assertEqual(
            len([m for m in metrics if m.get('MetricName') == 'QuerySuccesses']), 5)
        # There should be 5 'EngineExecutionTime' metrics
        self.assertEqual(len([m for m in metrics if m.get(
            'MetricName') == 'EngineExecutionTime']), 5)
        # There should be 5 'DataScanned' metrics
        self.assertEqual(
            len([m for m in metrics if m.get('MetricName') == 'DataScanned']), 5)
        # There should be 5 'EstimatedCost' metrics
        self.assertEqual(
            len([m for m in metrics if m.get('MetricName') == 'EstimatedCost']), 5)
        # And there should be 5 'QueryFailures' metrics
        self.assertEqual(
            len([m for m in metrics if m.get('MetricName') == 'QueryFailures']), 5)


if __name__ == '__main__':
    unittest.main()
