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

package org.trustedanalytics.atk.plugins.clusteringcoefficient

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }
import org.trustedanalytics.atk.plugins.graphconversions.GraphConversions
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.apache.spark.frame.FrameRdd
import org.apache.spark.graphx.lib.atk.plugins.ClusteringCoefficient
import org.apache.spark.graphx.{ Edge => GraphXEdge, PartitionStrategy, Graph }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Return value for the clustering coefficient runner
 * @param vertexOutput vertex data with the local clustering coefficient placed in the specified property.
 * @param globalClusteringCoefficient The global clustering coefficient of the input graph.
 */
case class ClusteringCoefficientRunnerReturn(vertexOutput: Option[FrameRdd], globalClusteringCoefficient: Double)

/**
 * Provides a method for running clustering coefficient on a graph using graphx. The result is a new graph with the
 * local clustering coefficient placed as a vertex property.
 */
object ClusteringCoefficientRunner extends Serializable {

  /**
   * Run clustering coefficient analysis of a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param outputPropertyLabel Optional name of the vertex property for storing local clustering coefficients.
   * @param inputEdgeLabels Optional list of edge labels to consider for clustering coefficient computation.
   * @return Vertices and edges for the output graph.
   */

  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], outputPropertyLabel: Option[String], inputEdgeLabels: Option[Set[String]]): ClusteringCoefficientRunnerReturn = {

    // clustering coefficient is an undirected graph algorithm, so the input graph should
    // have the directed edge (b,a) present whenever the directed edge (a,b) is present... furthermore,
    // graphx expects one edge to be present ... from Min(a,b) to Max(a,b)
    val canonicalEdges: RDD[GBEdge] =
      inEdges.filter(gbEdge => gbEdge.tailPhysicalId.asInstanceOf[Long] < gbEdge.headPhysicalId.asInstanceOf[Long])

    val filteredEdges: RDD[GBEdge] = if (inputEdgeLabels.isEmpty) {
      canonicalEdges
    }
    else {
      canonicalEdges.filter(edge => inputEdgeLabels.get.contains(edge.label))
    }

    // convert to graphX vertices
    val graphXVertices: RDD[(Long, Null)] =
      inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], null))

    val graphXEdges: RDD[GraphXEdge[Long]] = filteredEdges.map(edge => GraphConversions.createGraphXEdgeFromGBEdge(edge))

    // create graphx Graph instance from graphx vertices and edges
    val graph = Graph[Null, Long](graphXVertices, graphXEdges)
      .partitionBy(PartitionStrategy.RandomVertexCut)

    // run graphx clustering coefficient implementation

    val (newGraph, globalClusteringCoefficient) = ClusteringCoefficient.run(graph)

    val vertexOutput = if (outputPropertyLabel.nonEmpty) {
      val outputProperty = outputPropertyLabel.get
      // extract vertices and edges from graphx graph instance
      val intermediateVertices: RDD[(Long, Double)] = newGraph.vertices

      val schema = inVertices.aggregate(FrameSchemaAggregator.zeroValue)(FrameSchemaAggregator.seqOp, FrameSchemaAggregator.combOp)
        .addColumnIfNotExists(outputProperty, DataTypes.float64)

      val rowWrapper = new RowWrapper(schema)

      // Join the intermediate vertex/edge rdds with input vertex/edge rdd's to append the triangleCount attribute
      val outputRows = inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], rowWrapper.create(gbVertex)))
        .join(intermediateVertices)
        .map({ case (_, (row, coefficient)) => rowWrapper(row).setValue(outputProperty, coefficient) })

      Some(new FrameRdd(schema, outputRows))
    }
    else {
      None
    }

    ClusteringCoefficientRunnerReturn(vertexOutput, globalClusteringCoefficient)
  }

}
