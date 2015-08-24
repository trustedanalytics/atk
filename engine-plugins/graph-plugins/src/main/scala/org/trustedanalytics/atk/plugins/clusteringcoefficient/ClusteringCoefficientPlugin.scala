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

package org.trustedanalytics.atk.plugins.clusteringcoefficient

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, StorageFormats, DomainJsonProtocol }
import org.trustedanalytics.atk.domain.graph.{ GraphTemplate, GraphEntity, GraphReference }
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

case class ClusteringCoefficientArgs(@ArgDoc("<TBD>") graph: GraphReference,
                                     @ArgDoc("""The name of the new property to which each
vertex's local clustering coefficient will be written.
If this option is not specified, no output frame will be produced and only
the global clustering coefficient will be returned.""") outputPropertyName: Option[String],
                                     @ArgDoc("""If this list is provided,
only edges whose labels are included in the given
set will be considered in the clustering coefficient calculation.
In the default situation (when no list is provided), all edges will be used
in the calculation, regardless of label.
It is required that all edges that enter into the clustering coefficient
analysis be undirected.""") inputEdgeLabels: Option[List[String]] = None) {

  require(graph != null, "graph is required")
  require(outputPropertyName != null, "output property name should not be null")
  require(inputEdgeLabels != null, "list of edge labels should not be null")

  def inputEdgeSet: Option[Set[String]] =
    if (inputEdgeLabels.isEmpty) {
      None
    }
    else {
      Some(inputEdgeLabels.get.toSet)
    }
}

/**
 * Result of clustering coefficient calculation.
 * @param globalClusteringCoefficient The global clustering coefficient of the graph.
 * @param frame If local clustering coefficients are requested, a reference to the frame with local clustering
 *              coefficients stored at properties at each vertex.
 */
case class ClusteringCoefficientResult(globalClusteringCoefficient: Double, frame: Option[FrameReference] = None)

/** Json conversion for arguments and return value case classes */
object ClusteringCoefficientJsonFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val CCFormat = jsonFormat3(ClusteringCoefficientArgs)
  implicit val CCResultFormat = jsonFormat2(ClusteringCoefficientResult)
}
import ClusteringCoefficientJsonFormat._

@PluginDoc(oneLine = "Coefficient of graph with respect to labels.",
  extended = """Calculates the clustering coefficient of the graph with repect to an (optional) set of labels.

Pulls graph from underlying store, calculates degrees and writes them into the property specified,
and then writes the output graph to the underlying store.

.. warning::

    THIS FUNCTION IS FOR UNDIRECTED GRAPHS.
    If it is called on a directed graph, its output is NOT guaranteed to calculate
    the local directed clustering coefficients.

|
**Clustering Coefficients**

The clustering coefficient of a graph provides a measure of how tightly
clustered an undirected graph is.
Informally, if the edge relation denotes "friendship", the clustering
coefficient of the graph is the probability that two people are friends given
that they share a common friend.

More formally:

.. math::

    cc(G)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
    E \} \| }{\| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\} \in E \} \|}


Analogously, the clustering coefficient of a vertex provides a measure of how
tightly clustered that vertex's neighborhood is.
Informally, if the edge relation denotes "friendship", the clustering
coefficient at a vertex :math:`v` is the probability that two acquaintances of
:math:`v` are themselves friends.

More formally:

.. math::

    cc(v)  = \frac{ \| \{ (u,v,w) \in V^3: \ \{u,v\}, \{u, w\}, \{v,w \} \in \
    E \} \| }{\| \{ (u,v,w) \in V^3: \ \{v, u \}, \{v, w\} \in E \} \|}


The toolkit provides the function clustering_coefficient which computes both
local and global clustering coefficients for a given undirected graph.

For more details on the mathematics and applications of clustering
coefficients, see http://en.wikipedia.org/wiki/Clustering_coefficient.

""",
  returns = """Dictionary of the global clustering coefficient of the graph or,
if local clustering coefficients are requested, a reference to the frame with local
clustering coefficients stored at properties at each vertex.""")
class ClusteringCoefficientPlugin extends SparkCommandPlugin[ClusteringCoefficientArgs, ClusteringCoefficientResult] {

  override def name: String = "graph/clustering_coefficient"

  override def numberOfJobs(arguments: ClusteringCoefficientArgs)(implicit invocation: Invocation): Int = 6

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def execute(arguments: ClusteringCoefficientArgs)(implicit invocation: Invocation): ClusteringCoefficientResult = {
    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds
    val ccOutput = ClusteringCoefficientRunner.run(gbVertices, gbEdges, arguments.outputPropertyName, arguments.inputEdgeSet)
    if (ccOutput.vertexOutput.isDefined) {
      val newFrame = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("clustering coefficient results"))) {
        newFrame => newFrame.save(ccOutput.vertexOutput.get)
      }
      ClusteringCoefficientResult(ccOutput.globalClusteringCoefficient, Some(newFrame))
    }
    else {
      ClusteringCoefficientResult(ccOutput.globalClusteringCoefficient)
    }

  }

}
