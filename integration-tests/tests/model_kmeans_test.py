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
import uuid     # for generating unique model names

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelKMeansTest(unittest.TestCase):
    def testKMeans(self):
        """basic KMeans train + piggyback model last_read_date"""
        print "define csv file"
        csv = ta.CsvFile("/datasets/KMeansTestFile.csv", schema= [('data', ta.float64),
                                                             ('name', str)], skip_header_lines=1)

        print "create frame"
        frame = ta.Frame(csv)

        print "Initializing a KMeansModel object"
        k = ta.KMeansModel(name='myKMeansModel')
        t0 = k.last_read_date
        t1 = k.last_read_date
        #print "t0=%s" % t0.isoformat()
        self.assertEqual(t0, t1)

        print "Training the model on the Frame"
        k.train(frame,['data'],[2.0])
        t2 = k.last_read_date
        self.assertLess(t1, t2)

    # Tests creating a kmeans model and verifying that it's listed in get_model_names()
    def test_create_kmeans_model(self):
        model_name = str(uuid.uuid1()).replace('-','_')

        print "create kmeans model named: " + str(model_name)
        model = ta.KMeansModel(name=model_name)

        self.assertTrue(model_name in ta.get_model_names(), model_name + " should be in the list of models")

        # Delete the model to clean up after the test
        self.assertTrue(1 == ta.drop_models(model_name), "drop_models() should have deleted one model.")

    # Tests trying to create a kmeans model with the same name as an existing model
    def test_create_kmeans_model_with_duplicte_model_name(self):
        model_name = str(uuid.uuid1()).replace('-','_')

        print "create kmeans model named: " + str(model_name)
        model1 = ta.KMeansModel(name=model_name)

        self.assertTrue(model_name in ta.get_model_names(), model_name + " should be in the list of models")

        print "try to create another model with the same name"
        with self.assertRaises(Exception):
            model2 = ta.KMeansModel(name=model_name)

        # Delete the model to clean up after the test
        self.assertTrue(1 == ta.drop_models(model_name), "drop_models() should have deleted one model.")

    # Tests trying to create a kmeans model with the same name as an existing frame
    def test_create_kmeans_model_with_duplicte_frame_name(self):
        frame_name = str(uuid.uuid1()).replace('-','_')

        # Create frame
        print "Create frame named: " + frame_name
        ta.Frame(name=frame_name)

        print "try to create a model with the same name as the frame"
        with self.assertRaises(Exception):
            model = ta.KMeansModel(name=frame_name)
            ta.drop_models(frame_name)

        # Delete the frame to clean up after the test
        self.assertTrue(1 == ta.drop_frames(frame_name), "drop_frames() should have deleted one frame.")

    # Tests trying to create a kmeans model with the same name as an existing graph
    def test_create_kmeans_model_with_duplicte_graph_name(self):
        graph_name = str(uuid.uuid1()).replace('-','_')

        # Create graph
        print "Create graph named: " + graph_name
        ta.Graph(name=graph_name)
        self.assertTrue(graph_name in ta.get_graph_names(), graph_name + " should be in the list of graphs")

        print "try to create a model with the same name as the graph"
        with self.assertRaises(Exception):
            model = ta.KMeansModel(name=graph_name)

        # Delete the graph to clean up after the test
        self.assertTrue(1 == ta.drop_graphs(graph_name), "drop_graphs() should have deleted one graph.")


if __name__ == "__main__":
    unittest.main()
