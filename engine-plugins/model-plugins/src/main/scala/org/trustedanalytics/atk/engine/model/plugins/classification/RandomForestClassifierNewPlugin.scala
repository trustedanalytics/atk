/**
 *  Copyright (c) 2016 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation, CommandPlugin }
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/**
 * Create a 'new' instance of a Random Forest Classifier model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a Random Forest Classifier model.",
  extended = """Random Forest [1]_ is a supervised ensemble learning algorithm
which can be used to perform binary and multi-class classification.
The Random Forest Classifier model is initialized, trained on columns of a
frame, used to predict the labels of observations in a frame, and tests the
predicted labels against the true labels.
This model runs the MLLib implementation of Random Forest [2]_.
During training, the decision trees are trained in parallel.
During prediction, each tree's prediction is counted as vote for one class.
The label is predicted to be the class which receives the most votes.
During testing, labels of the observations are predicted and tested against the true labels
using built-in binary and multi-class Classification Metrics.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Random_forest
.. [2] https://spark.apache.org/docs/1.5.0/mllib-ensembles.html#random-forests
 """,
  returns = """A new instance of RandomForestClassifierModel""")
class RandomForestClassifierNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_classifier/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:random_forest_classifier")))
  }
}
