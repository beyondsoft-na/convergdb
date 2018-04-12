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

import athena_query_tracker

my_path = os.path.abspath(os.path.dirname(__file__))
ctl_file = os.path.join(my_path, "test_cloudtrail_log_event.json")

with open(ctl_file) as ctl_json:
    cloudtrail_log_event = json.load(ctl_json)


class TestAthenaQueryTracker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ['QUERY_TRACKING_TABLE'] = 'test'

    @classmethod
    def tearDownClass(cls):
        os.environ.pop('QUERY_TRACKING_TABLE')

    def test_process_ctl_event(self):
        query_ids = athena_query_tracker.process_ctl_event(
            cloudtrail_log_event)
        self.assertEqual(query_ids, ['a66e378e-52df-4e18-ba3a-32414eeb84ee',
                                     '4c196d87-9239-4b86-aac5-8ce074e31028',
                                     '43ff0a78-4f85-4cf1-8230-eafef93cdafe',
                                     'cfdbdc5f-9efa-4809-9bb5-8b1362553e2e',
                                     '28ff828f-ce39-4b16-8c1b-63aad01aa3f4',
                                     '16422c03-4970-48ac-b49b-3937c4aaa32b',
                                     'adf48dd4-1bf5-4a99-906c-ebc250218fa8',
                                     'e34df0df-67ef-487b-8e82-2fa7562aeb17',
                                     '915e82d5-56ea-4579-b06f-db462a70533e',
                                     '231915ac-7991-4ae4-82eb-7503a1dfc9da'])


if __name__ == '__main__':
    unittest.main()
