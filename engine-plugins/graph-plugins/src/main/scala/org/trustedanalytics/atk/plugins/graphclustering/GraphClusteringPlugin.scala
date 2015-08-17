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

package org.trustedanalytics.atk.plugins.graphclustering

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }
import org.trustedanalytics.atk.engine.graph.{ SparkGraph, GraphBuilderConfigFactory }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.domain.DomainJsonProtocol

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

case class GraphClusteringArgs(graph: GraphReference,
                               @ArgDoc("""Column name for the edge distance.""") edgeDistance: String)

/** Json conversion for arguments and return value case classes */
object GraphClusteringFormat {
  import DomainJsonProtocol._
  implicit val hFormat = jsonFormat2(GraphClusteringArgs)
}

import GraphClusteringFormat._

/**
 * GraphClusteringPlugin implements the graph clustering algorithm on a graph.
 */
@PluginDoc(oneLine = "Build graph clustering over an initial titan graph.",
  extended = "<TBD>",
  returns = "A set of titan vertices and edges representing the internal clustering of the graph.")
class GraphClusteringPlugin extends SparkCommandPlugin[GraphClusteringArgs, UnitReturn] {

  override def name: String = "graph:titan/graph_clustering"
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: GraphClusteringArgs)(implicit invocation: Invocation): UnitReturn = {
    val graph: SparkGraph = arguments.graph
    val (vertices, edges) = graph.gbRdds
    val titanConfig = GraphBuilderConfigFactory.getTitanConfiguration(graph)

    new GraphClusteringWorker(titanConfig).execute(vertices, edges, arguments.edgeDistance)
  }
}
