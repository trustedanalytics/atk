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

package org.trustedanalytics.atk.plugins.trianglecount

import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._

/**
 * Variables for executing triangle count.
 */
case class TriangleCountArgs(@ArgDoc("""Reference to the graph object on which
to compute triangle count.""") graph: GraphReference,
                             @ArgDoc("""The name of output property to be
added to vertex/edge upon completion.""") output_property: String,
                             @ArgDoc("""The name of edge labels to be considered for triangle count.
Default is all edges are considered.""") input_edge_labels: Option[List[String]] = None) {
  require(!output_property.isEmpty, "Output property label must be provided")
}

/**
 * The result object
 * @param frameDictionaryOutput Name of the output graph
 */
case class TriangleCountResult(frameDictionaryOutput: Map[String, FrameReference])

/** Json conversion for arguments and return value case classes */
object TriangleCountJsonFormat {
  import DomainJsonProtocol._
  implicit val TCFormat = jsonFormat3(TriangleCountArgs)
  implicit val TCResultFormat = jsonFormat1(TriangleCountResult)
}

import TriangleCountJsonFormat._

@PluginDoc(oneLine = "Number of triangles among vertices of current graph.",
  extended = """** Experimental Feature **
Triangle Count.
Counts the number of triangles among vertices in an undirected graph.
If an edge is marked bidirectional, the implementation opts for canonical
orientation of edges hence counting it only once (similar to an
undirected graph).""",
  returns = """dict(label, Frame).
Dictionary containing the vertex type as the key and the corresponding
vertex's frame with a triangle_count column.
Call dictionary_name['label'] to get the handle to frame whose vertex
type is label.""")
class TriangleCountPlugin extends SparkCommandPlugin[TriangleCountArgs, TriangleCountResult] {

  override def name: String = "graph/graphx_triangle_count"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: TriangleCountArgs)(implicit invocation: Invocation) = 2

  override def execute(arguments: TriangleCountArgs)(implicit invocation: Invocation): TriangleCountResult = {

    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val tcRunnerArgs = TriangleCountRunnerArgs(arguments.output_property, arguments.input_edge_labels)

    // Call TriangleCountRunner to kick off Triangle Count computation on RDDs
    val (outVertices, outEdges) = TriangleCountRunner.run(gbVertices, gbEdges, tcRunnerArgs)

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new TriangleCountResult(frameRddMap.keys.map(label => {
      val result: FrameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }

}
