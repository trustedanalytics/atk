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
import trustedanalytics.core.admin as admin

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()


class GarbageCollectionTests(unittest.TestCase):
    _multiprocess_can_split_ = False  # should NOT run concurrently with other tests, as it runs GC with small stale age

    def test_gc_drop_stale_and_finalize(self):
        csv = ta.CsvFile("/datasets/dates.csv", schema= [('start', ta.datetime),
                                                         ('id', int),
                                                         ('stop', ta.datetime),
                                                         ('color', str)], delimiter=',')
        f2_name = "dates_two"
        if f2_name in ta.get_frame_names():
            ta.drop_frames(f2_name)

        f1 = ta.Frame(csv)
        f1e = f1.get_error_frame()
        self.assertIsNotNone(f1e)
        self.assertIsNone(f1e.name)
        f2 = ta.Frame(csv, name=f2_name)
        f2e = f2.get_error_frame()
        self.assertIsNotNone(f2e)
        self.assertIsNone(f2e.name)

        admin.drop_stale()  # first, normal drop_stale, nothing should change because these frames aren't old enough
        self.assertEqual("ACTIVE", f1.status)
        self.assertEqual("ACTIVE", f1e.status)
        self.assertEqual("ACTIVE", f2.status)
        self.assertEqual("ACTIVE", f2e.status)
        # print "f1.status=%s, f2.status=%s" % (f1.status, f2.status)

        admin.finalize_dropped()  # nothing is dropped, so nothing so be finalized
        self.assertEqual("ACTIVE", f1.status)
        self.assertEqual("ACTIVE", f1e.status)
        self.assertEqual("ACTIVE", f2.status)
        self.assertEqual("ACTIVE", f2e.status)

        admin.drop_stale("1ms")  # now drop with very tiny age, so non-name f1 should get dropped
        self.assertEqual("DROPPED", f1.status)
        self.assertEqual("DROPPED", f1e.status)
        self.assertEqual("ACTIVE", f2.status)
        self.assertEqual("ACTIVE", f2e.status)
        # print "f1.status=%s, f2.status=%s" % (f1.status, f2.status)

        admin.finalize_dropped()  # on f1 and f1e are dropped, so only they should be finalized
        self.assertEqual("FINALIZED", f1.status)
        self.assertEqual("FINALIZED", f1e.status)
        self.assertEqual("ACTIVE", f2.status)
        self.assertEqual("ACTIVE", f2e.status)
        # print "f1.status=%s, f2.status=%s" % (f1.status, f2.status)


if __name__ == "__main__":
    unittest.main()
