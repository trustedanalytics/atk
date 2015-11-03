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


package org.trustedanalytics.atk.plugins.trianglecount

import org.trustedanalytics.atk.plugins.graphconversions.GraphConversions
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.graphx.lib.{ TriangleCount => GraphXTriangleCount }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Arguments for the TriangleCountRunnerArgs
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperties List of prior properties to consider for graph computation
 */
case class TriangleCountRunnerArgs(posteriorProperty: String,
                                   priorProperties: Option[List[String]])

/**
 * Provides a method for running triangle count on a graph using graphx. The result is a new graph with the
 * posterior value placed as a vertex property.
 */
object TriangleCountRunner extends Serializable {

  /**
   * Run pagerank on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of pagerank.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: TriangleCountRunnerArgs): (RDD[GBVertex], RDD[GBEdge]) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputEdgeLabels = args.priorProperties

    // Only select edges as specified in inputEdgeLabels
    val filteredEdges: RDD[GBEdge] = inputEdgeLabels match {
      case None => inEdges
      case _ => inEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => GraphConversions.createGraphXEdgeFromGBEdge(edge, canonicalOrientation = true)).distinct()

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx trianglecount implementation
    val newGraph: Graph[Int, Long] = GraphXTriangleCount.run(graph)

    // extract vertices and edges from graphx graph instance
    val intermediateVertices: RDD[(Long, Property)] = newGraph.vertices.map({
      case (physicalId, triangleCount) => (physicalId, Property(outputPropertyLabel, triangleCount))
    })

    // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the triangleCount attribute
    val outVertices: RDD[GBVertex] = inVertices
      .map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], gbVertex))
      .join(intermediateVertices)
      .map({ case (_, (vertex, property)) => vertex.copy(properties = vertex.properties + property) })

    (outVertices, inEdges)
  }
}
