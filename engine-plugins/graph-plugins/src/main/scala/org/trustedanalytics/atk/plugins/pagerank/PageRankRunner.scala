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


package org.trustedanalytics.atk.plugins.pagerank

import org.trustedanalytics.atk.plugins.graphconversions.GraphConversions
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.graphx.lib.{ PageRank => GraphXPageRank }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Arguments for the PageRankRunnerArgs
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperties List of prior properties to consider for graph computation
 * @param maxIterations Maximum number of iterations to execute.
 * @param resetProbability Random reset probability
 * @param convergenceTolerance Tolerance allowed at convergence (smaller values tend to yield accurate results)
 */
case class PageRankRunnerArgs(posteriorProperty: String,
                              priorProperties: Option[List[String]],
                              maxIterations: Option[Int],
                              resetProbability: Option[Double],
                              convergenceTolerance: Option[Double])

/**
 * Provides a method for running pagerank on a graph using graphx. The result is a new graph with the pagerank
 * posterior placed in a new vertex and edge property on each vertex and edge respectively.
 */
object PageRankRunner extends Serializable {

  /**
   * Run pagerank on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of pagerank.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: PageRankRunnerArgs): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputEdgeLabels = args.priorProperties
    val maxIterations: Int = args.maxIterations.getOrElse(PageRankDefaults.maxIterationsDefault)
    val resetProbability: Double = args.resetProbability.getOrElse(PageRankDefaults.resetProbabilityDefault)
    val convergenceTolerance: Double = args.convergenceTolerance.getOrElse(PageRankDefaults.convergenceToleranceDefault)

    // Only select edges as specified in inputEdgeLabels
    val filteredEdges: RDD[GBEdge] = inputEdgeLabels match {
      case None => inEdges
      case _ => inEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.cache().map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => GraphConversions.createGraphXEdgeFromGBEdge(edge))

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx pagerank implementation
    val newGraph: Graph[Double, Double] = args.convergenceTolerance match {
      case None => GraphXPageRank.run(graph, maxIterations, resetProbability)
      case _ => GraphXPageRank.runUntilConvergence(graph, convergenceTolerance, resetProbability)
    }

    // extract vertices and edges from graphx graph instance
    val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
      case (physicalId, pageRank) => (physicalId, Property(outputPropertyLabel, pageRank))
    })

    val edgePropertyPairs: RDD[((Long, Long), Property)] =
      newGraph.edges.map(edge => ((edge.srcId, edge.dstId), Property(outputPropertyLabel, edge.attr)))

    // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the pagerank attribute
    val outVertices: RDD[GBVertex] = inVertices
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(intermediateVertices)
      .map({ case (_, (vertex, property)) => vertex.copy(properties = vertex.properties + property) })

    val outEdges: RDD[GBEdge] = inEdges
      .map(gbEdge => ((gbEdge.tailPhysicalId.asInstanceOf[Long], gbEdge.headPhysicalId.asInstanceOf[Long]), gbEdge))
      .join(edgePropertyPairs)
      .map({ case (_, (edge, property)) => edge.copy(properties = edge.properties + property) })

    (outVertices, outEdges)
  }
}
