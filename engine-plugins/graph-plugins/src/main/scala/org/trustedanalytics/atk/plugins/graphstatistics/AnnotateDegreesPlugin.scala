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

package org.trustedanalytics.atk.plugins.graphstatistics

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphTemplate, GraphReference }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }
import org.trustedanalytics.atk.engine.graph.GraphBuilderConfigFactory
import org.trustedanalytics.atk.engine.graph._
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{ SparkConf, SparkContext }
import java.util.UUID
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

import scala.concurrent.Await

case class AnnotateDegreesArgs(graph: GraphReference,
                               @ArgDoc("""The name of the new property.
The degree is stored in this property.""") outputPropertyName: String,
                               @ArgDoc("""Indicator for the definition of degree to be used for the
calculation.
Permitted values:

*   "out" (default value) : Degree is calculated as the out-degree.
*   "in" : Degree is calculated as the in-degree.
*   "undirected" : Degree is calculated as the undirected degree.
    (Assumes that the edges are all undirected.)
   
Any prefix of the strings "out", "in", "undirected" will select the
corresponding option.""") degreeOption: Option[String] = None,
                               @ArgDoc("""If this list is provided, only edges whose labels are
included in the given set will be considered in the degree calculation.
In the default situation (when no list is provided), all edges will be used
in the degree calculation, regardless of label.""") inputEdgeLabels: Option[List[String]] = None) {
  require(!outputPropertyName.isEmpty, "Output property label must be provided")

  // validate arguments

  def degreeMethod: String = degreeOption.getOrElse("out")
  require("out".startsWith(degreeMethod) || "in".startsWith(degreeMethod) || "undirected".startsWith(degreeMethod),
    "degreeMethod should be prefix of 'in', 'out' or 'undirected', not " + degreeMethod)

  def inputEdgeSet: Option[Set[String]] =
    if (inputEdgeLabels.isEmpty) {
      None
    }
    else {
      Some(inputEdgeLabels.get.toSet)
    }

  def useUndirected() = "undirected".startsWith(degreeMethod)
  def useInDegree() = "in".startsWith(degreeMethod)
  def useOutDegree() = "out".startsWith(degreeMethod)
}

case class AnnotateDegreesReturn(frameDictionaryOutput: Map[String, FrameReference])

import DomainJsonProtocol._
import spray.json._

/** Json conversion for arguments and return value case classes */
object AnnotateDegreesJsonFormat {

  implicit val ADArgsFormat = jsonFormat4(AnnotateDegreesArgs)
  implicit val ADReturnFormat = jsonFormat1(AnnotateDegreesReturn)
}

import AnnotateDegreesJsonFormat._

/* Documentation updated from rst file. 20150723
@PluginDoc(oneLine = "Calculates the degree of each vertex with respect to an (optional) set of labels.",
  extended = """Pulls graph from underlying store, calculates degrees and writes them into the property specified,
and then writes the output graph to the underlying store.
*/

@PluginDoc(oneLine = "Make new graph with degrees.",
  extended = """Creates a new graph which is the same as the input graph, with the addition
that every vertex of the graph has its :term:`degree` stored in a
user-specified property.

**Degree Calculation**

A fundamental quantity in graph analyses is the degree of a vertex:
The degree of a vertex is the number of edges adjacent to it.

For a directed edge relation, a vertex has both an out-degree (the number of
edges leaving the vertex) and an in-degree (the number of edges entering the
vertex).

The toolkit provides this routine for calculating the degrees of vertices.
This calculation could be performed with a Gremlin query on smaller datasets
because Gremlin queries cannot be executed on a distributed scale.
The |PACKAGE| routine ``annotate_degrees`` can be executed at distributed scale.

In the presence of edge weights, vertices can have weighted degrees: The
weighted degree of a vertex is the sum of weights of edges adjacent to it.
Analogously, the weighted in-degree of a vertex is the sum of the weights of
the edges entering it, and the weighted out-degree is the sum
of the weights of the edges leaving the vertex.

The toolkit provides `annotate_weighted_degrees <annotate_weighted_degrees.html>`_
for the distributed calculation of weighted vertex degrees.""",
  returns = """Dictionary containing the vertex type as the key and the corresponding
vertex's frame with a column storing the annotated degree for the vertex
in a user specified property.
Call dictionary_name['label'] to get the handle to frame whose vertex type
is label.""")
class AnnotateDegreesPlugin extends SparkCommandPlugin[AnnotateDegreesArgs, AnnotateDegreesReturn] {

  override def name: String = "graph/annotate_degrees"

  override def numberOfJobs(arguments: AnnotateDegreesArgs)(implicit invocation: Invocation): Int = 4

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: AnnotateDegreesArgs)(implicit invocation: Invocation): AnnotateDegreesReturn = {

    val degreeMethod: String = arguments.degreeMethod
    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val vertexDegreePairs: RDD[(GBVertex, Long)] = if (arguments.useUndirected()) {
      UnweightedDegrees.undirectedDegreesByEdgeLabel(gbVertices, gbEdges, arguments.inputEdgeSet)
    }
    else if (arguments.useInDegree()) {
      UnweightedDegrees.inDegreesByEdgeLabel(gbVertices, gbEdges, arguments.inputEdgeSet)
    }
    else {
      UnweightedDegrees.outDegreesByEdgeLabel(gbVertices, gbEdges, arguments.inputEdgeSet)
    }

    val outVertices = vertexDegreePairs.map({
      case (v: GBVertex, d: Long) => GBVertex(physicalId = v.physicalId,
        gbId = v.gbId,
        properties = v.properties + Property(arguments.outputPropertyName, d))
    })

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new AnnotateDegreesReturn(frameRddMap.keys.map(label => {
      val result: FrameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by annotated degrees operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }
}
