#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import unittest
import trustedanalytics as ta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class FrameCategorySummaryTest(unittest.TestCase):
    """
    Test category_summary()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        schema = [("source",str),("target",str)]
        self.csv = ta.CsvFile("/datasets/noun_graph_small.csv", schema)

    def test_category_summary_topk(self):
        print "create frame"
        frame = ta.Frame(self.csv)

        print "compute category summary"
        cm = frame.categorical_summary(('source', {'top_k' : 2}))

        expected_result = {u'categorical_summary': [{u'column': u'source', u'levels': [
            {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'},
            {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'},
            {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            {u'percentage': 0.35714285714285715, u'frequency': 10, u'level': u'Other'}]}]}

        self.assertEquals(cm, expected_result, "test_category_summary_topk expected_result %s got %s" % (expected_result, cm))

    def test_category_summary_threshold(self):
        print "create frame"
        frame = ta.Frame(self.csv)

        print "compute category summary"
        cm = frame.categorical_summary(('source', {'threshold' : 0.5}))

        expected_result = {u'categorical_summary': [{u'column': u'source', u'levels': [
            {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            {u'percentage': 1.0, u'frequency': 28, u'level': u'Other'}]}]}
        self.assertEquals(cm, expected_result, "test_category_summary_threshold expected_result %s got %s" % (expected_result, cm))


    def test_category_summary_both(self):
        print "create frame"
        frame = ta.Frame(self.csv)

        print "compute category summary"
        cm = frame.categorical_summary(('source', {'top_k' : 2}), ('target', {'threshold' : 0.5}))

        expected_result = {u'categorical_summary': [{u'column': u'source', u'levels': [
            {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'},
            {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'},
            {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            {u'percentage': 0.35714285714285715, u'frequency': 10, u'level': u'Other'}]}, {u'column': u'target',
                                                                                           u'levels': [
                                                                                               {u'percentage': 0.0,
                                                                                                u'frequency': 0,
                                                                                                u'level': u'Missing'},
                                                                                               {u'percentage': 1.0,
                                                                                                u'frequency': 28,
                                                                                                u'level': u'Other'}]}]}

        self.assertEquals(cm, expected_result, "test_category_summary_both expected %s got %s" % (expected_result, cm))


    def test_category_summary_none(self):
        print "create frame"
        frame = ta.Frame(self.csv)

        print "compute category summary"
        cm = frame.categorical_summary('source','target')

        expected_result = {u'categorical_summary': [{u'column': u'source', u'levels': [
            {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'thing'},
            {u'percentage': 0.32142857142857145, u'frequency': 9, u'level': u'abstraction'},
            {u'percentage': 0.25, u'frequency': 7, u'level': u'physical_entity'},
            {u'percentage': 0.10714285714285714, u'frequency': 3, u'level': u'entity'},
            {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            {u'percentage': 0.0, u'frequency': 0, u'level': u'Other'}]}, {u'column': u'target', u'levels': [
            {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'thing'},
            {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'physical_entity'},
            {u'percentage': 0.07142857142857142, u'frequency': 2, u'level': u'entity'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'variable'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'unit'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'substance'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'subject'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'set'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'reservoir'},
            {u'percentage': 0.03571428571428571, u'frequency': 1, u'level': u'relation'},
            {u'percentage': 0.0, u'frequency': 0, u'level': u'Missing'},
            {u'percentage': 0.5357142857142857, u'frequency': 15, u'level': u'Other'}]}]}

        self.assertEquals(cm, expected_result, "test_category_summary_none expected %s got %s" % (expected_result, cm))
if __name__ == "__main__":
    unittest.main()
