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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, CommandPlugin, Invocation, PluginDoc }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARXJsonProtocol._

/**
 * Create a 'new' instance of a AutoRegressive Exogenous model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a AutoRegressive Exogenous model.",
  extended =
    """An Autoregressive (AR) model [1]_ is a representation of a type of random process; as such,
it describes certain time-varing processes in nature, economics, etc.  The term autoregressive model
specifies that the ouput variable lineraly on its own previous value and on a stochastic term
(a stochastic - an imperfectly predictable - term); thus the model is in the form of a stochastic
difference equation.
The Autoregressive Exogeneous (ARX) model add the use of exogeneous inputs to predict future values.

.. rubric: footnotes

.. [1] https://en.wikipedia.org/wiki/Autoregressive_model
    """.stripMargin,
  returns = """A new instance of ARXModel""")
class ARXNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {

  override def name: String = "model:arx/new"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:arx")))
  }
}
