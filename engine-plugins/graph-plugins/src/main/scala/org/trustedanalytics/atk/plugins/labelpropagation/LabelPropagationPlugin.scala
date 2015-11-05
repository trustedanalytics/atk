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

package org.trustedanalytics.atk.plugins.labelpropagation

import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.ApiMaturityTag.ApiMaturityTag
import org.trustedanalytics.atk.engine.plugin.ApiMaturityTag._
import org.trustedanalytics.atk.graphbuilder.elements.Property
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import spray.json._
import DomainJsonProtocol._

/**
 * Variables for executing label propagation.
 */
case class LabelPropagationArgs(graph: GraphReference,
                                @ArgDoc("""Number of super-steps before the algorithm terminates. Default = 10""") maxSteps: Option[Int] = None,
                                @ArgDoc("""The name of the column containing the propagated label value.""") outputVertexPropertyName: Option[String] = None) {
  require(graph != null, "graph is required")

  def getVertexPropertyName: String = {
    outputVertexPropertyName.getOrElse("propagatedLabel")
  }

  def getMaxSteps: Int = {
    val value = maxSteps.getOrElse(10)
    if (value < 1) 10 else value
  }
}

case class LabelPropagationReturn(frameDictionaryOutput: Map[String, FrameReference])

/** Json conversion for arguments and return value case classes */
object LabelPropagationJsonFormat {
  import DomainJsonProtocol._
  implicit val LPArgsFormat = jsonFormat3(LabelPropagationArgs)
  implicit val LPReturnFormat = jsonFormat1(LabelPropagationReturn)
}

import LabelPropagationJsonFormat._

@PluginDoc(oneLine = "Implements the label propagation computation on a graph by invoking graphx api.",
  extended = """
  The algorithm follows the next steps:
    At initial condition, nodes carry a label that denotes the community they belong. Belonging to a community changes, based on the labels that the neighboring nodes possess. This change is subject to the maximum number of labels within one degree of the nodes. Every node is initialized with a unique label then the labels diffuse through the network. Consequently, densely connected groups reach a common label quickly. When many such dense (consensus) groups are created throughout the network, they continue to expand outwards until it is possible to do so.[1]
    The process in 5 steps:[1]
     1. Initialize the labels at all nodes in the network. For a given node x, Cx (0) = x.
     2. Set t = 1.
     3. Arrange the nodes in the network in a random order and set it to X.
     4. For each x in X chosen in that specific order, let Cx(t) = f(Cxi1(t), ...,Cxim(t),Cxi(m+1) (t − 1), ...,Cxik (t − 1)). f here returns the label occurring with the highest frequency among neighbours.
     5. If every node has a label that the maximum number of their neighbours have, then stop the algorithm. Else, set t = t + 1 and go to (3).

     for more information see: http://arxiv.org/abs/0709.2938""",
  returns = """The original graph with the additional label for each vertex""")
class LabelPropagationPlugin extends SparkCommandPlugin[LabelPropagationArgs, LabelPropagationReturn] {
  override def name: String = "graph/graphx_label_propagation"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def apiMaturityTag: Option[ApiMaturityTag] = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: LabelPropagationArgs)(implicit invocation: Invocation): LabelPropagationReturn = {

    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val inputVertices: RDD[Long] = gbVertices.map(vertex => vertex.physicalId.asInstanceOf[Long])
    val inputEdges = gbEdges.map(edge => (edge.tailPhysicalId.asInstanceOf[Long], edge.headPhysicalId.asInstanceOf[Long]))

    val labeledGraph = LabelPropagationDefault.run(inputVertices, inputEdges, arguments.getMaxSteps)
    val labeledRdd = labeledGraph.map({
      case (vertexId, calculatedLabel) => (vertexId, Property(arguments.getVertexPropertyName, calculatedLabel))
    })

    val resultsGraph = LabelPropagationDefault.mergeResults(labeledRdd, gbVertices)
    val resultsFrameRdd = FrameRdd.toFrameRddMap(resultsGraph)

    new LabelPropagationReturn(resultsFrameRdd.keys.map(label => {
      val result: FrameReference = engine.frames.tryNewFrame(
        CreateEntityArgs(description = Some("created by label propagation operation"))) {
          newOutputFrame: FrameEntity =>
            val frameRdd = resultsFrameRdd(label)
            newOutputFrame.save(frameRdd)
        }
      (label, result)
    }).toMap)

  }

}
