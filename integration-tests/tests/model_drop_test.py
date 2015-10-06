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

# show full stack traces
ta.errors.show_details = True
ta.loggers.set_api()
# TODO: port setup should move to a super class
if ta.server.port != 19099:
    ta.server.port = 19099
ta.connect()

class ModelDropTest(unittest.TestCase):
    """
    Tests drop_model() and drop() with model names/objects.

    This is a build-time test so it needs to be written to be as fast as possible:
    - Only use the absolutely smallest toy data sets, e.g 20 rows rather than 500 rows
    - Tests are ran in parallel
    - Tests should be short and isolated.
    """
    _multiprocess_can_split_ = True

    # Tests ta.drop_model() with the model name
    def test_drop_model_by_name(self):
        model_name = "test_model"

        # Create model and verify that it's in the get_model_names() list
        print "create model"
        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of models")

        # Drop model by name
        print "dropping model by name"
        ta.drop_models(model_name)
        self.assertFalse(model_name in ta.get_model_names(), model_name + " should not exist in the list of models")

    # Tests ta.drop_model() with the model proxy object
    def test_drop_model_by_object(self):
        model_name = "test_model"

        # Create model and verify that it's in the get_model_names() list
        print "create model"
        model = ta.KMeansModel(name=model_name)
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of models")

        # Drop model using the model object
        print "dropping model by entity"
        ta.drop_models(model)
        self.assertFalse(model_name in ta.get_model_names(), model_name + " should not exist in the list of models")

    # Tests the generic ta.drop() using the model proxy object
    def test_generic_drop_by_object(self):
        # drop existing model
        for model_name in ta.get_model_names():
            ta.drop(model_name)

        print "create model"
        model_name = "test_model"
        model = ta.KMeansModel(name=model_name)

        # Check that the model we just created now exists
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of model names")

        print "drop model"
        ta.drop(model)

        # check that the gramodelph no longer exists
        self.assertFalse(model_name in ta.get_model_names(), model_name + " should not exist in the list of models")

    # Tests the generic ta.drop() using the model name
    def test_generic_drop_by_object(self):
        # drop existing model
        for model_name in ta.get_model_names():
            ta.drop(model_name)

        print "create model"
        model_name = "test_model"
        model = ta.KMeansModel(name=model_name)

        # Check that the model we just created now exists
        self.assertTrue(model_name in ta.get_model_names(), model_name + " should exist in the list of model names")

        print "drop model"
        ta.drop(model_name)

        # check that the model no longer exists
        self.assertFalse(model_name in ta.get_model_names(), model_name + " should not exist in the list of model")

if __name__ == "__main__":
    unittest.main()
