/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation, CommandPlugin }
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/**
 * Create a 'new' instance of a Random Forest Regressor model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a Random Forest Regressor model.",
  extended = """Random Forest [1]_ is a supervised ensemble learning algorithm
used to perform regression.
A Random Forest Regressor model is initialized, trained on columns of a frame,
and used to predict the value of each observation in the frame.
This model runs the MLLib implementation of Random Forest [2]_.
During training, the decision trees are trained in parallel.
During prediction, the average over-all tree's predicted value is the predicted
value of the random forest.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Random_forest
.. [2] https://spark.apache.org/docs/1.3.0/mllib-ensembles.html""",
  returns = """A new instance of RandomForestRegressor Model""")
class RandomForestRegressorNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_regressor/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:random_forest_regressor")))
  }
}
