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


class LastReadDateUpdatesTests(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_access_refreshes_frames(self):
        """Tests that some actions do or do not update the last_read_date entity property"""
        csv = ta.CsvFile("/datasets/dates.csv", schema= [('start', ta.datetime),
                                                         ('id', int),
                                                         ('stop', ta.datetime),
                                                         ('color', str)], delimiter=',')
        name = "update_last_read"
        if name in ta.get_frame_names():
            ta.drop_frames(name)
        f = ta.Frame(csv, name=name)  # name it, to save it from the other GC blasting test in here
        t0 = f.last_read_date
        t1 = f.last_read_date
        #print "t0=%s" % t0.isoformat()
        self.assertEqual(t0, t1)

        f.schema  # schema, or other meta data property reads, should not update the last read date
        t2 = f.last_read_date
        #print "t2=%s" % t2.isoformat()
        self.assertEqual(t0, t2)

        f.inspect()  # inspect should update the last read date
        t3 = f.last_read_date
        #print "t3=%s" % t3.isoformat()
        self.assertLess(t2,t3)

        f.copy()  # copy should update the last read date
        t4 = f.last_read_date
        #print "t4=%s" % t4.isoformat()
        self.assertLess(t3,t4)

        f.bin_column('id', [3, 5, 8])
        t5 = f.last_read_date
        #print "t5=%s" % t5.isoformat()
        self.assertLess(t4,t5)

    # testing graph and model last_read_date is piggybacked on other tests which already do the
    # heavy lifting of proper construction.  See graph_pagerank_tests.py and model_kmeans_test.py


if __name__ == "__main__":
    unittest.main()
