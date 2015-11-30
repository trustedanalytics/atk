# vim: set encoding=utf-8

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


class FrameFilterTest(unittest.TestCase):
    """
    Tests Frame and VertexFrame filter operations. For VertexFrame filtering, the test also ensures that
    the dangling edges are also removed.
    """
    _multiprocess_can_split_ = True

    def setUp(self):
        csv = ta.CsvFile("/datasets/oregon-cities.csv", schema= [('rank', ta.int32),
                                                       ('city', str),
                                                       ('population_2013', str),
                                                       ('pop_2010', str),
                                                       ('change', str),
                                                       ('county', str)], delimiter='|')
        self.frame = ta.Frame(csv)
        self.graph = ta.Graph()
        self.graph.define_vertex_type('city')
        self.graph.define_vertex_type('population_2013')
        self.graph.define_edge_type('rank', 'city', 'population_2013', directed=False)

        self.graph.vertices['city'].add_vertices(self.frame, 'city')
        self.graph.vertices['population_2013'].add_vertices(self.frame, 'population_2013')
        self.graph.edges['rank'].add_edges(self.frame, 'city', 'population_2013', ['rank'], create_missing_vertices=False)

        self.vertex_frame = self.graph.vertices['city']

    # Tests the Frame's filter operation by verifying that only specified rows remain after applying a filter.
    def test_frame_filter(self):
        rows = self.frame.take(self.frame.row_count)
        original_edge_count = self.graph.edges['rank'].row_count
        original_vertex_frame_row_count = self.graph.vertices['population_2013'].row_count
        count = 0

        # Count number of rows with a population_2013 > 100,000
        for row in rows:
            if int(row[2].replace(',','')) > 100000:
                count += 1

        # Filter the frame to keep row with a population_2013 > 100000
        self.frame.filter(lambda row: int(row['population_2013'].replace(',','')) > 100000)

        # Check that we have the expected number of rows
        filtered_row_count = self.frame.row_count
        self.assertEqual(count, filtered_row_count)

        # Verify frame data to ensure everything left in the frame has a population_2013 > 100000
        rows = self.frame.take(self.frame.row_count)
        for row in rows:
            self.assertTrue(int(row[2].replace(',','')) > 100000)

        # Edge count shouldn't have changed
        self.assertEqual(original_edge_count, self.graph.edges['rank'].row_count)

        # Vertex frame row count should not have changed
        self.assertEqual(original_vertex_frame_row_count, self.graph.vertices['population_2013'].row_count)

    # Tests the VertexFrame's filter operation by verifying that only specified rows remain after applying
    # a filter, and that dangling edges have also been removed.
    def test_vertex_frame_filter(self):
        rows = self.vertex_frame.take(self.vertex_frame.row_count)
        count = 0
        vertex_ids = [None] * self.vertex_frame.row_count

        # Count number of rows where the city starts with 'B'
        for row in rows:
            if row[2][0]=='B':
                vertex_ids[count] = row[0]
                count += 1

        vertex_ids = vertex_ids[:count]

        # Filter vertex frame to keep rows that start with 'B'
        self.vertex_frame.filter(lambda  row: row['city'][0] == 'B')

        # Check row count
        filtered_row_count = self.vertex_frame.row_count
        self.assertEqual(count, filtered_row_count)

        # Verify vertex frame to ensure that all the cities left start with 'B'
        rows = self.vertex_frame.take(self.vertex_frame.row_count)
        for row in rows:
            self.assertTrue(row[2][0]=='B')

        # Check edges
        self.assertEqual(count, self.graph.edges['rank'].row_count)
        for edge_row in self.graph.edges['rank'].take(self.graph.edges['rank'].row_count):
            source_vertex_id = edge_row[1]
            self.assertTrue(source_vertex_id in vertex_ids)



if __name__ == "__main__":
    unittest.main()
