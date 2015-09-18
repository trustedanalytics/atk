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
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol._
/**
 * Create a 'new' instance of a Linear Regression model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a Linear Regression model.",
  extended = """
**Regression using Linear Regression**

Linear Regression[1]_ is used to model the relationship between a scalar dependent variable and one or more independent variables. The user may initialize a LinearRegressionModel,
train the model on columns of a frame and use the trained model to predict the value of the dependent variable given the independent observations of a frame. This model
runs the MLLib implementation of LinearRegression[2]_ with the SGD[3]_ optimizer.
                
.. [1] https://en.wikipedia.org/wiki/Linear_regression
.. [2] https://spark.apache.org/docs/1.3.0/mllib-linear-methods.html#linear-least-squares-lasso-and-ridge-regression
.. [3] https://en.wikipedia.org/wiki/Stochastic_gradient_descent
    """)
class LinearRegressionWithSGDNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:linear_regression/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:linear_regression")))
  }
}
