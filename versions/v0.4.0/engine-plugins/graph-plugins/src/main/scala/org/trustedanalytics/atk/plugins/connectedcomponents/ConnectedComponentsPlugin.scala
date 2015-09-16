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

package org.trustedanalytics.atk.plugins.connectedcomponents

import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.graphbuilder.elements.Property
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }
import org.apache.spark.rdd.RDD
import spray.json._
import DomainJsonProtocol._

/**
 * Variables for executing connected components.
 */
case class ConnectedComponentsArgs(graph: GraphReference,
                                   @ArgDoc("""The name of the column containing the connected component value.""") outputProperty: String) {
  require(!outputProperty.isEmpty, "Output property label must be provided")
}

case class ConnectedComponentsReturn(frameDictionaryOutput: Map[String, FrameReference])

/** Json conversion for arguments and return value case classes */
object ConnectedComponentsJsonFormat {
  import DomainJsonProtocol._
  implicit val CCArgsFormat = jsonFormat2(ConnectedComponentsArgs)
  implicit val CCReturnFormat = jsonFormat1(ConnectedComponentsReturn)
}

import ConnectedComponentsJsonFormat._

@PluginDoc(oneLine = "Implements the connected components computation on a graph by invoking graphx api.",
  extended = """Pulls graph from underlying store, sends it off to the ConnectedComponentGraphXDefault,
and then writes the output graph back to the underlying store.

|
**Connected Components (CC)**

Connected components are disjoint subgraphs in which all vertices are
connected to all other vertices in the same component via paths, but not
connected via paths to vertices in any other component.
The connected components algorithm uses message passing along a specified edge
type to find all of the connected components of a graph and label each edge
with the identity of the component to which it belongs.
The algorithm is specific to an edge type, hence in graphs with several
different types of edges, there may be multiple, overlapping sets of connected
components.

The algorithm works by assigning each vertex a unique numerical index and
passing messages between neighbors.
Vertices pass their indices back and forth with their neighbors and update
their own index as the minimum of their current index and all other indices
received.
This algorithm continues until there is no change in any of the vertex
indices.
At the end of the alorithm, the unique levels of the indices denote the
distinct connected components.
The complexity of the algorithm is proportional to the diameter of the graph.
""",
  returns = """Dictionary containing the vertex type as the key and the corresponding
  vertex's frame with a connected component column.
  Call dictionary_name['label'] to get the handle to frame whose vertex type is label.""")
class ConnectedComponentsPlugin extends SparkCommandPlugin[ConnectedComponentsArgs, ConnectedComponentsReturn] {
  override def name: String = "graph/graphx_connected_components"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: ConnectedComponentsArgs)(implicit invocation: Invocation): ConnectedComponentsReturn = {

    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val inputVertices: RDD[Long] = gbVertices.map(gbvertex => gbvertex.physicalId.asInstanceOf[Long])
    val inputEdges = gbEdges.map(gbedge => (gbedge.tailPhysicalId.asInstanceOf[Long], gbedge.headPhysicalId.asInstanceOf[Long]))

    // Call ConnectedComponentsGraphXDefault to kick off ConnectedComponents computation on RDDs
    val intermediateVertices = ConnectedComponentsGraphXDefault.run(inputVertices, inputEdges)
    val connectedComponentRDD = intermediateVertices.map({
      case (vertexId, componentId) => (vertexId, Property(arguments.outputProperty, componentId))
    })

    val outVertices = ConnectedComponentsGraphXDefault.mergeConnectedComponentResult(connectedComponentRDD, gbVertices)

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new ConnectedComponentsReturn(frameRddMap.keys.map(label => {
      val result: FrameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }

}
