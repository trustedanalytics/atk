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

package org.trustedanalytics.atk.engine.model.plugins

import org.trustedanalytics.atk.domain.model.{ ModelReference, RenameModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

// TODO: shouldn't be a Spark Plugin, doesn't need Spark

/**
 * Rename a model in the database
 */
class RenameModelPlugin extends SparkCommandPlugin[RenameModelArgs, ModelReference] {

  /**
   * The name of the command, e.g. graph/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model/rename"

  /**
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RenameModelArgs)(implicit invocation: Invocation): ModelReference = {
    // dependencies (later to be replaced with dependency injection)
    val models = engine.models

    // validate arguments
    val modelRef = arguments.model
    val newName = arguments.newName

    // run the operation and save results
    models.renameModel(modelRef, newName)
  }
}
