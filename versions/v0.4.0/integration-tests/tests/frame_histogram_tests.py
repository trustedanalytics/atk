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
import numpy

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

_multiprocess_can_split_ = True

class FrameHistogramTests(unittest.TestCase):
    """
    Test frame.append

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        self.csv = ta.CsvFile("/datasets/movie.csv", schema= [('user', ta.int32),
                                                        ('vertex_type', str),
                                                        ('movie', ta.int32),
                                                        ('rating', ta.int32),
                                                        ('splits', str)])
        print "creating frame"
        self.frame = ta.Frame(self.csv)

    def test_histogram_equal_width(self):
        h = self.frame.histogram('user', 4)
        self.assertEquals(h.cutoffs, [-2347.0, -1759.5, -1172.0, -584.5, 3.0])
        self.assertEquals(h.hist, [1.0, 5.0, 1.0, 13.0])
        numpy.testing.assert_array_almost_equal(h.density, [1.0/20.0, 5.0/20.0, 1.0/20.0, 13.0/20.0])

    def test_histogram_generate_num_bins(self):
        # the number of bins should be equal to the square root of the row count rounded down
        h = self.frame.histogram('user')
        self.assertEquals(len(h.hist), math.floor(math.sqrt(self.frame.row_count)))

    def test_histogram_with_weights(self):
        h = self.frame.histogram('user', 4, 'rating')
        self.assertEquals(h.cutoffs, [-2347.0, -1759.5, -1172.0, -584.5, 3.0])
        self.assertEquals(h.hist, [3.0, 13.0, 1.0, 29.0])
        numpy.testing.assert_array_almost_equal(h.density, [3/46.0, 13/46.0, 1/46.0, 29/46.0])

    def test_histogram_equal_depth(self):
        h = self.frame.histogram('user', 4, bin_type='equaldepth')
        self.assertEquals(h.cutoffs,  [-2347.0, -1209.0, 1.0, 2.0, 3.0])
        self.assertEquals(h.hist, [5.0, 5.0, 5.0, 5.0])
        numpy.testing.assert_array_almost_equal(h.density,  [0.25, 0.25, 0.25, 0.25])


if __name__ == "__main__":
    unittest.main()
