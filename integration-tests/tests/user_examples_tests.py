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



class UserExamples(unittest.TestCase):
    def test_frame(self):
        import trustedanalytics.examples.frame as frame_test
        frame_test.run("/datasets/cities.csv", ta)
        assert True

    def test_movie_graph_small(self):
        import trustedanalytics.examples.movie_graph_small as mgs
        vars = mgs.run("/datasets/movie_data_random.csv", ta)

        assert vars["frame"].row_count == 2
        assert vars["frame"].name == "MGS_frame" and vars["graph"].name == "MGS_graph"
        assert vars["graph"].vertex_count == 4
        assert vars["graph"].edge_count == 2


    def test_pr(self):
        import trustedanalytics.examples.pr as pr
        vars = pr.run("/datasets/movie_data_random.csv", ta)

        assert vars["frame"].row_count == 20
        assert vars["frame"].name == "PR_frame" and vars["graph"].name == "PR_graph"
        assert vars["graph"].vertex_count == 29
        assert vars["graph"].edge_count == 20
        assert vars["result"]["vertex_dictionary"]["user_id"].row_count == 18
        assert vars["result"]["vertex_dictionary"]["movie_id"].row_count == 11
        assert vars["result"]["edge_dictionary"]["rating"].row_count == 20
