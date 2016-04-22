/**
 *  Copyright (c) 2015 Intel Corporation 
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

package org.trustedanalytics.atk.engine.daal.plugins.dimensionalityreduction

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, CommandPlugin, Invocation, PluginDoc }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Create a 'new' instance of an Intel DAAL Principal Components model
 */
@PluginDoc(oneLine = "Create a 'new' instance of an Intel DAAL Principal Components model.",
  extended = """Principal component analysis [1]_ is a statistical algorithm
that converts possibly correlated features to linearly uncorrelated variables
called principal components.
The number of principal components is less than or equal to the number of
original variables.
This implementation of computing Principal Components is done by Singular
Value Decomposition [2]_ of the data, providing the user with an option to
mean center the data.
The Principal Components model is initialized; trained on
specifying the observation columns of the frame and the number of components;
used to predict principal components.
The Intel DAAL Singular Value Decomposition [3]_ implementation has been used for
this, with additional features to 1) mean center the data during train and
predict and 2) compute the t-squared index during prediction.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Principal_component_analysis
.. [2] https://en.wikipedia.org/wiki/Singular_value_decomposition
.. [3] https://software.intel.com/en-us/daal""",
  returns = """A new instance of PrincipalComponentsModel""")
class DaalPrincipalComponentsNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_principal_components/new"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:daal_principal_components")))
  }
}

