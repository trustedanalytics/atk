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

package org.trustedanalytics.atk.engine.model.plugins.classification.glm

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Create a 'new' instance of logistic regression model.",
  extended =
    """
Logistic Regression [1]_ is a widely used supervised binary and multi-class classification algorithm.
The Logistic Regression model is initialized, trained on columns of a frame, predicts the labels
of observations, and tests the predicted labels against the true labels.
This model runs the MLLib implementation of Logistic Regression [2]_, with enhanced features |EM|
trained model summary statistics; Covariance and Hessian matrices; ability to specify the frequency
of the train and test observations.
Testing performance can be viewed via built-in binary and multi-class Classification Metrics.
It also allows the user to select the optimizer to be used - L-BFGS [3]_ or SGD [4]_.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Logistic_regression
.. [2] https://spark.apache.org/docs/1.3.0/mllib-linear-methods.html#logistic-regression
.. [3] https://en.wikipedia.org/wiki/Limited-memory_BFGS
.. [4] https://en.wikipedia.org/wiki/Stochastic_gradient_descent
    """,
  returns = """A new instance of LogisticRegressionModel""")
class LogisticRegressionNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:logistic_regression/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:logistic_regression")))
  }
}
