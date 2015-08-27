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

package org.trustedanalytics.atk.plugins.pagerank

import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity }
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }

import spray.json._

/**
 * Parameters for executing page rank.
 * @param graph Reference to the graph object on which to compute pagerank.
 */
case class PageRankArgs(@ArgDoc("""Reference to the graph object on which
to compute pagerank.""") graph: GraphReference,
                        @ArgDoc("""Name of the property to which pagerank
value will be stored on vertex and edge.""") output_property: String,
                        @ArgDoc("""List of edge labels to consider for pagerank computation.
Default is all edges are considered.""") input_edge_labels: Option[List[String]] = None,
                        @ArgDoc("""The maximum number of iterations that will be invoked.
The valid range is all positive int.
Invalid value will terminate with vertex page rank set to reset_probability.
Default is 20.""") max_iterations: Option[Int] = None,
                        @ArgDoc("""The probability that the random walk of a page is reset.
Default is 0.15.""") reset_probability: Option[Double] = None,
                        @ArgDoc("""The amount of change in cost function that will be tolerated at
convergence.
If this parameter is specified, max_iterations is not considered as a stopping condition.
If the change is less than this threshold, the algorithm exits earlier.
The valid value range is all float and zero.
Default is 0.001.""") convergence_tolerance: Option[Double] = None) {
  require(!output_property.isEmpty, "Output property label must be provided")
}

/**
 * Companion object holds the default values.
 */
object PageRankDefaults {
  val maxIterationsDefault = 20
  val resetProbabilityDefault = 0.15d
  val convergenceToleranceDefault = 0.001d
}

case class PageRankResult(vertexDictionaryOutput: Map[String, FrameReference], edgeDictionaryOutput: Map[String, FrameReference])

/** Json conversion for arguments and return value case classes */
object PageRankJsonFormat {
  import DomainJsonProtocol._
  implicit val PRFormat = jsonFormat6(PageRankArgs)
  implicit val PRResultFormat = jsonFormat2(PageRankResult)
}

import PageRankJsonFormat._

@PluginDoc(oneLine = "Determining which vertices are the most important.",
  extended = """Pulls graph from underlying store, sends it off to the PageRankRunner,
and then writes the output graph back to the underlying store.

Right now it is using only Titan for graph storage. Other backends including Parquet will be supported later.

** Experimental Feature **
The `PageRank algorithm <http://en.wikipedia.org/wiki/PageRank>`_.

**Basics and Background**

*PageRank* is a method for determining which vertices in a directed graph are
the most central or important.
*PageRank* gives each vertex a score which can be interpreted as the
probability that a person randomly walking along the edges of the graph will
visit that vertex.

The calculation of *PageRank* is based on the supposition that if a vertex has
many vertices pointing to it, then it is "important",
and that a vertex grows in importance as more important vertices point to it.
The calculation is based only on the network structure of the graph and makes
no use of any side data, properties, user-provided scores or similar
non-topological information.

*PageRank* was most famously used as the core of the Google search engine for
many years, but as a general measure of :term:`centrality` in a graph, it has
other uses to other problems, such as :term:`recommendation systems` and
analyzing predator-prey food webs to predict extinctions.

**Background references**

*   Basic description and principles: `Wikipedia\: PageRank`_
*   Applications to food web analysis: `Stanford\: Applications of PageRank`_
*   Applications to recommendation systems: `PLoS\: Computational Biology`_

**Mathematical Details of PageRank Implementation**

Our implementation of *PageRank* satisfies the following equation at each
vertex :math:`v` of the graph:

.. math::

    PR(v) = \frac {\rho}{n} + \rho \left( \sum_{u\in InSet(v)} \
    \frac {PR(u)}{L(u)} \right)

Where:
    |   :math:`v` |EM| a vertex
    |   :math:`L(v)` |EM| outbound degree of the vertex v
    |   :math:`PR(v)` |EM| *PageRank* score of the vertex v
    |   :math:`InSet(v)` |EM| set of vertices pointing to the vertex v
    |   :math:`n` |EM| total number of vertices in the graph
    |   :math:`\rho` - user specified damping factor (also known as reset
        probability)

Termination is guaranteed by two mechanisms.

*   The user can specify a convergence threshold so that the algorithm will
    terminate when, at every vertex, the difference between successive
    approximations to the *PageRank* score falls below the convergence
    threshold.
*   The user can specify a maximum number of iterations after which the
    algorithm will terminate.

.. _Wikipedia\: PageRank: http://en.wikipedia.org/wiki/PageRank
.. _Stanford\: Applications of PageRank: http://web.stanford.edu/class/msande233/handouts/lecture8.pdf
.. _PLoS\: Computational Biology:
    http://www.ploscompbiol.org/article/fetchObject.action?uri=info%3Adoi%2F10.1371%2Fjournal.pcbi.1000494&representation=PDF""",
  returns = """dict((vertex_dictionary, (label, Frame)), (edge_dictionary,(label,Frame))).
Dictionary containing a dictionary of labeled vertices and labeled edges.
For the vertex_dictionary the vertex type is the key and the corresponding
vertex's frame with a new column storing the page rank value for the vertex
Call vertex_dictionary['label'] to get the handle to frame whose vertex
type is label.
For the edge_dictionary the edge type is the key and the corresponding
edge's frame with a new column storing the page rank value for the edge
Call edge_dictionary['label'] to get the handle to frame whose edge type
is label.""")
class PageRankPlugin extends SparkCommandPlugin[PageRankArgs, PageRankResult] {

  override def name: String = "graph/graphx_pagerank"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: PageRankArgs)(implicit invocation: Invocation): PageRankResult = {

    // Get the graph
    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val prRunnerArgs = PageRankRunnerArgs(arguments.output_property,
      arguments.input_edge_labels,
      arguments.max_iterations,
      arguments.reset_probability,
      arguments.convergence_tolerance)

    // Call PageRankRunner to kick off PageRank computation on RDDs
    val (outVertices, outEdges) = PageRankRunner.run(gbVertices, gbEdges, prRunnerArgs)

    val edgeFrameRddMap = FrameRdd.toFrameRddMap(outEdges, outVertices)

    val edgeMap = edgeFrameRddMap.keys.map(edgeLabel => {
      val edgeFrame: FrameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = edgeFrameRddMap(edgeLabel)
        newOutputFrame.save(frameRdd)
      }
      (edgeLabel, edgeFrame)
    }).toMap

    val vertexFrameRddMap = FrameRdd.toFrameRddMap(outVertices)

    val vertexMap = vertexFrameRddMap.keys.map(vertexLabel => {
      val vertexFrame: FrameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) { newOutputFrame: FrameEntity =>
        val frameRdd = vertexFrameRddMap(vertexLabel)
        newOutputFrame.save(frameRdd)
      }
      (vertexLabel, vertexFrame)
    }).toMap

    new PageRankResult(vertexMap, edgeMap)

  }

}
