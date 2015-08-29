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

import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.graphbuilder.elements.{ GBVertex, Property }
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import org.trustedanalytics.atk.domain.graph.{ GraphTemplate, GraphEntity, GraphReference }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

case class AnnotateWeightedDegreesArgs(graph: GraphReference,
                                       @ArgDoc("<TBD>") outputPropertyName: String,
                                       @ArgDoc("<TBD>") degreeOption: Option[String] = None,
                                       @ArgDoc("<TBD>") inputEdgeLabels: Option[List[String]] = None,
                                       @ArgDoc("<TBD>") edgeWeightProperty: Option[String] = None,
                                       @ArgDoc("<TBD>") edgeWeightDefault: Option[Double] = None) {

  // validate arguments

  require(!outputPropertyName.isEmpty, "Output property label must be provided")
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

  def getDefaultEdgeWeight(): Double = {
    if (edgeWeightProperty.isEmpty) {
      1.0d
    }
    else {
      edgeWeightDefault.getOrElse(1.0d)
    }
  }
}

case class AnnotateWeightedDegreesReturn(frameDictionaryOutput: Map[String, FrameReference])

import DomainJsonProtocol._
import spray.json._

/** Json conversion for arguments and return value case classes */
object AnnotateWeightedDegreesJsonFormat {

  implicit val AWDArgsFormat = jsonFormat6(AnnotateWeightedDegreesArgs)
  implicit val AWDReturnFormat = jsonFormat1(AnnotateWeightedDegreesReturn)
}

import AnnotateWeightedDegreesJsonFormat._

@PluginDoc(oneLine = "Calculates the weighted degree of each vertex with respect to an (optional) set of labels.",
  extended = """Pulls graph from underlying store, calculates weighted degrees and writes them into the property
specified, and then writes the output graph to the underlying store.

**Degree Calculation**

A fundamental quantity in graph analyses is the degree of a vertex:
The degree of a vertex is the number of edges adjacent to it.

For a directed edge relation, a vertex has both an out-degree (the number of
edges leaving the vertex) and an in-degree (the number of edges entering the
vertex).

The toolkit provides a routine `annotate_degrees <annotate_degrees.html>`_
for calculating the degrees of vertices.
This calculation could be performed with a Gremlin query on smaller datasets
because Gremlin queries cannot be executed on a distributed scale.
The |PACKAGE| routine ``annotate_degrees`` can be executed at distributed scale.

In the presence of edge weights, vertices can have weighted degrees: The
weighted degree of a vertex is the sum of weights of edges adjacent to it.
Analogously, the weighted in-degree of a vertex is the sum of the weights of
the edges entering it, and the weighted out-degree is the sum
of the weights of the edges leaving the vertex.

The toolkit provides this routine for the distributed calculation of weighted
vertex degrees.""")
class AnnotateWeightedDegreesPlugin extends SparkCommandPlugin[AnnotateWeightedDegreesArgs, AnnotateWeightedDegreesReturn] {

  override def name: String = "graph/annotate_weighted_degrees"

  override def numberOfJobs(arguments: AnnotateWeightedDegreesArgs)(implicit invocation: Invocation): Int = 4

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: AnnotateWeightedDegreesArgs)(implicit invocation: Invocation): AnnotateWeightedDegreesReturn = {

    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val inputEdgeSet = arguments.inputEdgeSet
    val weightPropertyOPtion = arguments.edgeWeightProperty
    val defaultEdgeWeight = arguments.getDefaultEdgeWeight()

    val vertexWeightPairs: RDD[(GBVertex, Double)] = if (arguments.useUndirected()) {
      WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }
    else if (arguments.useInDegree()) {
      WeightedDegrees.inWeightByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }
    else {
      WeightedDegrees.outDegreesByEdgeLabel(gbVertices, gbEdges, weightPropertyOPtion, defaultEdgeWeight, inputEdgeSet)
    }

    val outVertices = vertexWeightPairs.map({
      case (v: GBVertex, wd: Double) => GBVertex(physicalId = v.physicalId,
        gbId = v.gbId,
        properties = v.properties + Property(arguments.outputPropertyName, wd))
    })

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)

    new AnnotateWeightedDegreesReturn(frameRddMap.keys.map(label => {
      val result: FrameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by annotate weighted degrees operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = frameRddMap(label)
        newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap)

  }
}
