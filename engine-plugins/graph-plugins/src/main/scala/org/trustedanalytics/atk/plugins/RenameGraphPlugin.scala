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

package org.trustedanalytics.atk.plugins

import org.trustedanalytics.atk.domain.graph.{ GraphEntity, RenameGraphArgs }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Rename a graph in the database
 */
@PluginDoc(oneLine = "Rename a graph in the database.",
  extended = "")
class RenameGraphPlugin extends CommandPlugin[RenameGraphArgs, GraphEntity] {

  /**
   * The name of the command, e.g. graph/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph/rename"

  /**
   *
   * Rename a graph in the database
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RenameGraphArgs)(implicit invocation: Invocation): GraphEntity = {
    // dependencies (later to be replaced with dependency injection)
    val graphs = engine.graphs

    // validate arguments
    val graphRef = arguments.graph
    val graph = graphs.expectGraph(graphRef)
    val newName = arguments.newName

    // run the operation and save results
    graphs.renameGraph(graph, newName)
  }
}
