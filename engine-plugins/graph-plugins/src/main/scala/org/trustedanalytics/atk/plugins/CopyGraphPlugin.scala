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

package org.trustedanalytics.atk.plugins

import org.trustedanalytics.atk.domain.graph.{ CopyGraphArgs, GraphEntity }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Makes a copy of the existing graph
 */
@PluginDoc(oneLine = "Make a copy of the current graph.",
  extended = "",
  returns = "A copy of the original graph.")
class CopyGraphPlugin extends SparkCommandPlugin[CopyGraphArgs, GraphEntity] {

  /**
   * The name of the command, e.g. graph/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph/copy"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CopyGraphArgs)(implicit invocation: Invocation) = 3

  /**
   * Makes a copy of the graph in the database.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CopyGraphArgs)(implicit invocation: Invocation): GraphEntity = {
    // dependencies (later to be replaced with dependency injection)
    val graphs = engine.graphs

    // validate arguments
    val graphRef = arguments.graph
    val graph = graphs.expectGraph(graphRef)

    //run the copy operation
    graphs.copyGraph(graph, arguments.name)
  }
}
