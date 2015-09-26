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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.apache.spark.SparkContext._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

/**
 * Create a 'new' instance of a Support Vector Machine model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a Support Vector Machine model.",
  extended = """Support Vector Machine [1]_ is a supervised algorithm used to
perform binary classification.
A Support Vector Machine constructs a high dimensional hyperplane which is
said to achieve a good separation when a hyperplane has the largest distance
to the nearest training-data point of any class.
This model runs the MLLib implementation of SVM [2]_ with SGD [3]_ optimizer.
The SVMWithSGD model is initialized, trained on columns of a frame, used to
predict the labels of observations in a frame, and tests the predicted labels
against the true labels.
During testing, labels of the observations are predicted and tested against
the true labels using built-in binary Classification Metrics.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Support_vector_machine
.. [2] https://spark.apache.org/docs/1.3.0/mllib-linear-methods.html
.. [3] https://en.wikipedia.org/wiki/Stochastic_gradient_descent""")
class SVMWithSGDNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:svm/new"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:svm")))
  }
}
