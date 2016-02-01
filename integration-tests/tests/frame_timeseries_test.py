# vim: set encoding=utf-8

#
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
from datetime import date, datetime, timedelta

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

def generateDateTimeIndex (start, end, interval):
    current = start
    dateTimeIndex = []
    while (current <= end):
        dateTimeIndex.append(current.isoformat()+"Z")
        current += interval
    return dateTimeIndex

class FrameTimeSeriesTest(unittest.TestCase):
    """
    Test time series operations from python

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True
    

    def test_timeseries_001(self):
        dateCol = "dates"
        keyCol = "key"
        valueCol = "count"
        otherCol = "other"

        print "define csv file"
        schema = [(dateCol, ta.datetime), (keyCol, str), (valueCol, ta.float64), (otherCol, ta.int32)]
        csv = ta.CsvFile("/datasets/timeseriesdata.csv", schema=schema)
        frame = ta.Frame(csv)

        # Setup date time index from 1-1-2016 to 1-6-2016
        datetimeindex = generateDateTimeIndex(datetime(2016,1,1,12), datetime(2016,1,6,12), timedelta(days=1))

        # Create timeseries frame from observations
        ts = frame.timeseries_from_observations(datetimeindex, dateCol, keyCol, valueCol)
        
        # Check that we have the expected number of rows (one for each key - "a" and "b")
        self.assertEqual(2, ts.row_count)

        # Sort on key, and then check the frame contents
        ts.sort(keyCol)
        expected_ts_data = [["a", [1.0, 4.0, 3.0, 4.0, 4.0, 6.0]],
                            ["b", [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]]]
        self.assertEqual(expected_ts_data, ts.take(ts.row_count, 0))
        
        # Slice frame to just have 1-2-2016 to 1-4-2016
        ts_slice = ts.timeseries_slice(datetimeindex, "2016-01-02T12:00:00.000Z","2016-01-04T12:00:00.000Z")
        ts_slice.sort(keyCol)
        expected_ts_slice = [["a", [4.0, 3.0, 4.0]],
                             ["b", [3.0, 4.0, 5.0]]]
        self.assertEqual(expected_ts_slice, ts_slice.take(ts_slice.row_count, 0))





if __name__ == "__main__":
    unittest.main()
