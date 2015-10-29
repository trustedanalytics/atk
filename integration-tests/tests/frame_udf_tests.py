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

csv = ta.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ta.int32),
                                                         ('city', str),
                                                         ('population_2013', str),
                                                         ('pop_2010', str),
                                                         ('change', str),
                                                         ('county', str)], delimiter='|')


class FrameUdfTests(unittest.TestCase):
    """
    Tests APIs that use Python UDFs
    """

    _multiprocess_can_split_ = True

    def test_filter(self):
        frame = ta.Frame(csv)
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        frame.filter(lambda row: row.county == "Washington")
        self.assertEquals(frame.row_count, 4, "frame should have 4 rows after filtering")
        cities = frame.take(frame.row_count, columns="city")
        self.assertEquals(sorted(map(lambda f: str(f[0]), cities)), ["Beaverton", "Hillsboro", "Tigard","Tualatin"])

    def test_add_columns_and_copy_where(self):
        """
        Tests UDFs for add_columns and copy(where), and uses the vector type

        Changes the 2 population strings to a vector, and then uses the vector
        to compute the change, and then copy out all the incorrect ones
        """
        frame = ta.Frame(csv)
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        frame.add_columns(lambda row: [float(row['pop_2010'].translate({ord(','): None})),
                                       float(row['population_2013'].translate({ord(','): None}))],
                          ("vpops", ta.vector(2)))
        self.assertEquals(frame.row_count, 20, "frame should have 20 rows")
        self.assertEquals(frame.column_names, ['rank', 'city', 'population_2013', 'pop_2010', 'change', 'county', 'vpops'])
        frame.add_columns(lambda row: (row.vpops[1] - row.vpops[0])/row.vpops[0], ("comp_change", ta.float64))
        #print frame.inspect(20)
        bad_cities = frame.copy(columns=['city', 'change', 'comp_change'], where=lambda row: row.change != "%.2f%%" % round(100*row.comp_change, 2))
        self.assertEquals(bad_cities.column_names, ['city', 'change', 'comp_change'])
        self.assertEquals(bad_cities.row_count, 1)
        #print bad_cities.inspect()
        row = bad_cities.take(1)[0]
        row[2] = round(row[2], 5)
        self.assertEquals(row, [u'Tualatin', u'4.17%', 0.03167])  # should just be one bad one, Tualatin


if __name__ == "__main__":
    unittest.main()
