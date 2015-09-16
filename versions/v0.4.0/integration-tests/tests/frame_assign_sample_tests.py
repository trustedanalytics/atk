#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import trustedanalytics as ta
import math
from trustedanalytics.rest.command import CommandServerError

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

_multiprocess_can_split_ = True

class FrameAssignSampleTests(unittest.TestCase):
    """
    Test frame.assign_sample

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        # there's already a "splits" column in this data set, but for testing purposes, it doesn't affect anything
        print "define csv file"
        self.schema = [('user', ta.int32),
                         ('vertex_type', str),
                         ('movie', ta.int32),
                         ('rating', ta.int32),
                         ('splits', str)]
        self.csv = ta.CsvFile("/datasets/movie.csv", self.schema)

        print "creating frame"
        self.frame = ta.Frame(self.csv)

    def test_assign_sample_low_probabilities(self):
        try:
            f = self.frame.assign_sample(sample_percentages= [0.1, 0.2], sample_labels=None, output_column='fuBuddy', random_seed=None)
            self.fail("FAIL. Providing probabilities that do not sum to 1 should raise exception from assign_columns")
        except CommandServerError:
            pass

    def test_assign_sample_high_probabilities(self):
        try:
            f = self.frame.assign_sample(sample_percentages= [0.6, 0.5], sample_labels=None, output_column='fuBuddy', random_seed=None)
            self.fail("FAIL. Providing probabilities that do not sum to 1 should raise exception from assign_columns")
        except CommandServerError:
            pass

    def test_assign_sample_column_name(self):
        self.frame.assign_sample(sample_percentages= [0.1, 0.2, 0.4, 0.3], sample_labels=None, output_column='fuBuddy', random_seed=None)
        self.assertEqual(self.frame.column_names, [name for name, type in self.schema + [('fuBuddy', str)]])


if __name__ == "__main__":
    unittest.main()
