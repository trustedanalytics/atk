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

import sys
sys.path.insert(0, "/home/blbarker/dev/atk/python-client")
import unittest
import trustedanalytics as ta
import pytz
utc = pytz.UTC

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()


class DateTimeTests(unittest.TestCase):
    _multiprocess_can_split_ = True

    def setUp(self):
        print "define csv file"
        csv = ta.CsvFile("/datasets/dates.csv", schema= [('start', ta.datetime),
                                                         ('id', int),
                                                         ('stop', ta.datetime),
                                                         ('color', str)], delimiter=',')

        print "create frame"
        self.frame = ta.Frame(csv)

    def test_sort_datetime(self):
        f = self.frame.copy()
        #print "original"
        #print f.inspect(wrap=10)
        data0 = f.take(n=2)
        self.assertEqual("2010-05-01T19:09:01.000Z", data0[0][0])
        self.assertEqual("2010-08-15T19:00:34.000Z", data0[1][2])
        f.sort("stop")
        #print "sorted on stop"
        #print f.inspect(wrap=10)
        data1 = f.take(n=2)
        self.assertEqual("2010-09-01T19:01:20.000Z", data1[0][0])
        self.assertEqual("2010-06-15T19:00:35.000Z", data1[1][2])

    def test_udf_datetime(self):
        f = self.frame.copy()
        # Do a couple add_columns...

        # 1. Do a date diff by seconds
        f.add_columns(lambda row: (row.start - row.stop).total_seconds(), ('diff', ta.float64))
        #print f.inspect(wrap=10)
        data = f.take(n=5, columns=['diff'])
        self.assertEqual(-11836313, data[0][0])
        self.assertEqual(9417609, data[4][0])

        # 2. Add seconds to a datetime value
        def add_seconds(seconds, column):
            """Returns a row function which adds seconds to the named column"""
            s = seconds
            c = column
            from datetime import timedelta

            def add_s(row):
                return row[c] + timedelta(seconds=s)
            return add_s

        f.add_columns(add_seconds(20, "stop"), ("stop_plus_20", ta.datetime))
        print f.inspect(wrap=10)

        data = f.take(n=2, columns=['stop_plus_20'])
        self.assertEqual("2010-09-15T19:01:14.000Z", data[0][0])
        self.assertEqual("2010-08-15T19:00:54.000Z", data[1][0])

        # 3. Add a label according to date range
        def add_range_label(range_labels, column):
            """
            Returns a row function which adds a label to the row according to the value found in the named column

            range_labels is a dict of label -> (inclusive_range_start_datetime, exclusive_range_stop_datetime)
            """

            labels = range_labels
            c = column

            def add_s(row):
                for label, range_tuple in labels.items():
                    if range_tuple[0] <= row[c] < range_tuple[1]:
                        return label
                return None
            return add_s

        def m(year, month):
            """get datetime for the start of the given month"""
            return ta.datetime(year, month, 1, tzinfo=utc)

        ranges = {
            "winter": (m(2010, 1), m(2010, 4)),
            "spring": (m(2010, 4), m(2010, 7)),
            "summer": (m(2010, 7), m(2010, 10)),
            "fall": (m(2010, 10), m(2011, 1)),
        }

        f.add_columns(add_range_label(ranges, "start"), ("season", str))
        print f.inspect(wrap=10, columns=["start", "season"])
        data = map(lambda row: row[0], f.take(n=5, columns=['season']))
        mismatches = filter(lambda x: x[0] != x[1], zip(data, ['spring', 'spring', "summer", "summer", "summer"]))
        self.assertFalse(mismatches, str(mismatches))

    def test_download_datetime(self):
        df = self.frame.download()
        #print "Pandas DF:\n" + repr(df)
        self.assertEqual("2010-05-01T19:09:01.000Z", df["start"][0])
        self.assertEqual("2010-07-15T19:00:20.000Z", df["stop"][2])


if __name__ == "__main__":
    unittest.main()
