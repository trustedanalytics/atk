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

package org.trustedanalytics.atk.plugins.graphstatistics

import org.trustedanalytics.atk.graphbuilder.driver.spark.elements.{ GBEdge, GBVertex }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Object for calculating degree and its generalizations: Per-vertex statistics about the edges incident to that vertex.
 *
 * Currently supported statistics:
 * - per vertex in-degree (optionally restricted to a specified set of edge labels)
 * - per vertex out-degree (optionally restricted to a specified set of edge labels)
 * - per vertex undirected degree (optionally restricted to a specified set of edge labels)
 */
object UnweightedDegrees {

  private def degreeCalculation(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], calculateOutDegreeFlag: Boolean): RDD[(GBVertex, Long)] = {

    /*
     * To make sure that we handle degree 0 vertices correctly (an especially important case when there are multiple
     * kinds of edges) we must include the vertexRDD in the calculation as well as the edgeRDD.
     *
     * A further complication is that each edge knows only the vertex IDs of its endpoints, not the data stored at
     * those endpoints.
     *
     * The calculation proceeds by the aggregation of (vertex, degree) records keyed by the vertex ID.
     * Each vertex emits (vertexData, degree = 0) keyed by the vertex ID
     * Each edges emits (None, degree = 1) keyed by the vertex ID of the head (if in-degree) or tail (if out-degree)
     *
     * The aggregation sums the net degrees and takes the non-empty vertex data it is provided.
     * In a well-formed graph, vertex IDs are unique, and each vertex ID key will see exactly one record with
     * nonempty vertex data in its aggregation.
     */

    val vertexVDRs: RDD[(Any, VertexDegreeRecord)] = vertexRDD.map(gbVertex => (gbVertex.physicalId, VertexDegreeRecord(Some(gbVertex), 0L)))

    val edgeVDRs: RDD[(Any, VertexDegreeRecord)] = {
      if (calculateOutDegreeFlag)
        edgeRDD.map(e => (e.tailPhysicalId, VertexDegreeRecord(None, 1L)))
      else
        edgeRDD.map(e => (e.headPhysicalId, VertexDegreeRecord(None, 1L)))
    }

    val vdrs = vertexVDRs.union(edgeVDRs)

    val combinedVDRs: RDD[VertexDegreeRecord] =
      vdrs.combineByKey(x => x, mergeVertexAndDegrees, mergeVertexAndDegrees).map(_._2)

    // there will be a get on an empty option only if there exists a vertex in the EdgeRDD that is
    // not in the VertexRDD... if this happens something was wrong  with the incoming data

    combinedVDRs.map(vad => (vad.vertexOption.get, vad.degree))
  }

  private case class VertexDegreeRecord(vertexOption: Option[GBVertex], degree: Long)

  private def mergeVertexAndDegrees(vad1: VertexDegreeRecord, vad2: VertexDegreeRecord) = {
    require(vad1.vertexOption.isEmpty || vad2.vertexOption.isEmpty)
    val v = if (vad1.vertexOption.isDefined) vad1.vertexOption else vad2.vertexOption
    VertexDegreeRecord(v, vad1.degree + vad2.degree)
  }

  /**
   * Calculates the out-degree of each vertex using edges of all possible labels.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @return RDD of (Vertex, out-degree) pairs
   */
  def outDegrees(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge]): RDD[(GBVertex, Long)] = {
    degreeCalculation(vertexRDD, edgeRDD, calculateOutDegreeFlag = true)
  }

  /**
   * Calculates the in-degree of each vertex using edges of all possible labels.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @return RDD of (Vertex, in-degree) pairs
   */
  def inDegrees(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge]): RDD[(GBVertex, Long)] = {
    degreeCalculation(vertexRDD, edgeRDD, calculateOutDegreeFlag = false)
  }

  /**
   * Calculates the  undirected degree of each vertex using edges of all possible labels.
   * Assumes that all provided edge labels are for undirected edges.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @return RDD of (Vertex, degree) pairs
   */
  def undirectedDegrees(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge]): RDD[(GBVertex, Long)] = {
    /*
     * Because undirected edges are internally represented as bi-directional edge/anti-edge pairs,
     * this is simply the out degree calculation. A change of the representation would change this
     * calculation.
     */
    degreeCalculation(vertexRDD, edgeRDD, calculateOutDegreeFlag = true)
  }

  /**
   * Calculates the out-degree of each vertex using edges from a given set of edge labels.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param edgeLabels Set of edge labels for which to calculate out-degrees
   * @return RDD of (VertexID, out-degree with respect to the set of considered edge labels) pairs
   */
  def outDegreesByEdgeLabel(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], edgeLabels: Option[Set[String]]): RDD[(GBVertex, Long)] = {
    val filteredEdges = filterEdges(edgeRDD, edgeLabels)
    outDegrees(vertexRDD, filteredEdges)
  }

  /**
   * Calculates the in-degree of each vertex using edges from a given set of edge labels.
   *
   * @param vertexRDD RDD containing vertices
   * @param edgeRDD RDD containing edges
   * @param edgeLabels Set of dge label for which to calculate in-degrees
   * @return RDD of (VertexID, in-degree with respect to set of considered edge labels) pairs
   */
  def inDegreesByEdgeLabel(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], edgeLabels: Option[Set[String]]): RDD[(GBVertex, Long)] = {
    val filteredEdges = filterEdges(edgeRDD, edgeLabels)
    inDegrees(vertexRDD, filteredEdges)
  }

  /**
   * Calculates the  undirected degree of each vertex using edges from a given set of edge labels.
   * Assumes that all provided edge labels are for undirected edges.
   *
   * @param vertexRDD RDD of vertices
   * @param edgeRDD RDD of edges
   * @param edgeLabels Set of edge labels for which to calculate degrees
   * @return RDD of (VertexID, degree with respect to the set of considered edge labels) pairs
   */
  def undirectedDegreesByEdgeLabel(vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge], edgeLabels: Option[Set[String]]): RDD[(GBVertex, Long)] = {
    val filteredEdges = filterEdges(edgeRDD, edgeLabels)
    outDegrees(vertexRDD, filteredEdges)
  }

  private def filterEdges(edgeRDD: RDD[GBEdge], edgeLabels: Option[Set[String]]): RDD[GBEdge] = {
    if (edgeLabels.nonEmpty) {
      edgeRDD.filter(edge => edgeLabels.get.contains(edge.label))
    }
    else {
      edgeRDD
    }
  }
}
