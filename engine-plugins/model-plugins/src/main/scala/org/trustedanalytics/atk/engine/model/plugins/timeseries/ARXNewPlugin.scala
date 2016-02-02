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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Create a 'new' instance of a k-means model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a autoregressive exogenous model.",
  extended = """TBD""",
  returns = """A new instance of ARXModel""")
class ARXNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {

  override def name: String = "model:arx/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:arx")))
  }
}
