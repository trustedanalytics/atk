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

package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, CommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Create a 'new' instance of a Linear Regression model
 */

@PluginDoc(oneLine = "Create a 'new' instance of a Linear Regression model.",
  extended = """Linear Regression [1]_ is used to model the relationship between a scalar
dependent variable and one or more independent variables.
The Linear Regression model is initialized, trained on columns of a frame and
used to predict the value of the dependent variable given the independent
observations of a frame.
This model runs the DAAL implementation of Linear Regression [2]_ with
QR [3]_ decomposition.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Linear_regression
.. [2] https://software.intel.com/en-us/daal
.. [3] https://en.wikipedia.org/wiki/QR_decomposition""",
  returns = """A new instance of LinearRegressionModel"""
)
class DaalLinearRegressionNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/new"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    val models = engine.models
    models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:daal_linear_regression")))
  }
}
