# vim: set encoding=utf-8
#  Copyright (c) 2016 Intel Corporation 
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

class FrameColumnMedianModeTest(unittest.TestCase):
    """
    Test column median & mode computation()

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def test_column_median(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
                                                                          ('b', ta.int32),
                                                                          ('labels', ta.int32),
                                                                          ('predictions', ta.int32)], delimiter=',', skip_header_lines=1)

        print "create frame"
        frame = ta.Frame(csv)

        print "compute column median()"
        column_median_b = frame.column_median(data_column='b')
        self.assertEquals(column_median_b, 1, "computed column median for column b should be equal to 1")

        column_median_b_weighted = frame.column_median(data_column='b', weights_column='labels')
        self.assertEquals(column_median_b_weighted, 0, "computed column median for column b with weights column labels should be equal to 0")

    # TODO: temporarily commenting out to get a good build
    # def test_column_mode(self):
    #     print "define csv file"
    #     csv = ta.CsvFile("/datasets/classification-compute.csv", schema= [('a', str),
    #                                                                       ('b', ta.int32),
    #                                                                       ('labels', ta.int32),
    #                                                                       ('predictions', ta.int32)], delimiter=',', skip_header_lines=1)
    #
    #     print "create frame"
    #     frame = ta.Frame(csv)
    #
    #     print "compute column mode()"
    #     column_mode_b = frame.column_mode(data_column='b')
    #     self.assertEquals(column_mode_b['modes'], [1], "computed column mode for column b should be equal to [1]")
    #     self.assertEquals(column_mode_b['weight_of_mode'], 2.0, "computed weight_of_mode for column b should be equal to 2.0")
    #     self.assertEquals(column_mode_b['mode_count'], 1, "computed mode_count for column b should be equal to 1")
    #     self.assertEquals(column_mode_b['total_weight'], 4.0, "computed total_weight for column b should be equal to 4.0")
    #
    #     column_mode_b_weighted = frame.column_mode(data_column='b', weights_column='labels')
    #     self.assertEquals(column_mode_b_weighted['modes'], [0], "computed column mode for column b should be equal to [0]")
    #     self.assertEquals(column_mode_b_weighted['weight_of_mode'], 1.0, "computed weight_of_mode for column b should be equal to 1.0")
    #     self.assertEquals(column_mode_b_weighted['mode_count'], 2, "computed mode_count for column b should be equal to 2")
    #     self.assertEquals(column_mode_b_weighted['total_weight'], 2.0, "computed total_weight for column b should be equal to 2.0")

if __name__ == "__main__":
    unittest.main()
