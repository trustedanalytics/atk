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

package org.trustedanalytics.atk.giraph.plugins.model.cf

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Collaborative filtering recommend model - create a 'new' instance of this model
 */
@PluginDoc(oneLine = "Create a new Collaborative Filtering Recommend model.",
  extended = """For details about Collaborative Filter Recommend modelling,
see :ref:`Collaborative Filter <CollaborativeFilteringNewPlugin_Summary>`.""")
class CollaborativeFilteringNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    val models = engine.models
    models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:collaborativefiltering_new")))
  }
}
