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

package org.trustedanalytics.atk.plugins.sampling

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration
import org.trustedanalytics.atk.plugins.graphstatistics.UnweightedDegrees
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilderConfig, GraphBuilder }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import java.util.Random // scala.util.Random is not serializable ???

object VertexSampleSparkOps extends Serializable {

  /**
   * Produce a uniform vertex sample
   *
   * @param vertices the vertices to sample
   * @param size the specified sample size
   * @param seed optional random seed value
   * @return RDD containing the vertices in the sample
   */
  def sampleVerticesUniform(vertices: RDD[GBVertex], size: Int, seed: Option[Long]): RDD[GBVertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    // TODO: Currently, all vertices are treated the same.  This should be extended to allow the user to specify, for example, different sample weights for different vertex types.
    if (size >= vertices.count()) {
      vertices
    }
    else {
      val rand = checkSeed(seed)

      // create tuple of (sampleWeight, vertex)
      val weightedVertexRdd = vertices.map(vertex => (rand.nextDouble(), vertex))

      getTopVertices(weightedVertexRdd, size)
    }
  }

  /**
   * Produce a weighted vertex sample using the vertex degree as the weight
   *
   * This will result in a bias toward high-degree vertices.
   *
   * @param vertices the vertices to sample
   * @param edges RDD of all edges
   * @param size the specified sample size
   * @param seed optional random seed value
   * @return RDD containing the vertices in the sample
   */
  def sampleVerticesDegree(vertices: RDD[GBVertex], edges: RDD[GBEdge], size: Int, seed: Option[Long]): RDD[GBVertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    if (size >= vertices.count()) {
      vertices
    }
    else {
      val rand = checkSeed(seed)

      // create tuple of (vertexDegree, vertex)
      val vertexDegreeRdd = addVertexDegreeWeights(vertices, edges)

      // create tuple of (sampleWeight, vertex)
      val weightedVertexRdd = vertexDegreeRdd.map { case (vertexDegree, vertex) => (rand.nextDouble() * vertexDegree, vertex) }

      getTopVertices(weightedVertexRdd, size)
    }
  }

  /**
   * Produce a weighted vertex sample using the size of the degree histogram bin as the weight for a vertex, instead
   * of just the degree value itself
   *
   * This will result in a bias toward vertices with more frequent degree values.
   *
   * @param vertices the vertices to sample
   * @param edges RDD of all edges
   * @param size the specified sample size
   * @param seed optional random seed value
   * @return RDD containing the vertices in the sample
   */
  def sampleVerticesDegreeDist(vertices: RDD[GBVertex], edges: RDD[GBEdge], size: Int, seed: Option[Long]): RDD[GBVertex] = {
    require(size >= 1, "Invalid sample size: " + size)
    if (size >= vertices.count()) {
      vertices
    }
    else {
      val rand = checkSeed(seed)

      // create tuple of (vertexDegreeBinSize, vertex)
      val vertexDegreeRdd = addVertexDegreeDistWeights(vertices, edges)

      // create tuple of (sampleWeight, vertex)
      val weightedVertexRdd = vertexDegreeRdd.map { case (vertexDegreeBinSize, vertex) => (rand.nextDouble() * vertexDegreeBinSize, vertex) }

      getTopVertices(weightedVertexRdd, size)
    }
  }

  /**
   * Get the edges for the vertex induced subgraph
   *
   * @param vertices the set of sampled vertices from which to construct the vertex induced subgraph
   * @param edges the set of edges for the input graph
   * @return the edge RDD for the vertex induced subgraph
   */
  def vertexInducedEdgeSet(vertices: RDD[GBVertex], edges: RDD[GBEdge]): RDD[GBEdge] = {
    // TODO: Find more efficient way of doing this that does not involve collecting sampled vertices
    val vertexArray = vertices.map(v => v.physicalId).collect()
    edges.filter(e => vertexArray.contains(e.headPhysicalId) && vertexArray.contains(e.tailPhysicalId))
  }

  /**
   * Write graph to Titan via GraphBuilder
   *
   * @param vertices the vertices to write to Titan
   * @param edges the edges to write to Titan
   * @param titanConfig the config for Titan
   */
  def writeToTitan(vertices: RDD[GBVertex], edges: RDD[GBEdge], titanConfig: SerializableBaseConfiguration): Unit = {
    val gb = new GraphBuilder(new GraphBuilderConfig(new InputSchema(Seq.empty), List.empty, List.empty, titanConfig))
    gb.buildGraphWithSpark(vertices, edges)
  }

  /**
   * Check the optional seed and set if necessary
   *
   * @param seed optional random seed
   * @return a new Random instance
   */
  def checkSeed(seed: Option[Long]): Random = {
    seed match {
      case Some(l) => new Random(l)
      case _ => new Random
    }
  }

  /**
   * Get the top *size* number of vertices sorted by weight
   *
   * @param weightedVertexRdd RDD of (vertexWeight, vertex)
   * @param size number of vertices to take
   * @return RDD of vertices
   */
  def getTopVertices(weightedVertexRdd: RDD[(Double, GBVertex)], size: Int): RDD[GBVertex] = {
    // TODO: There is a bug with Spark top function that is resolved in Spark 1.1.0, so use less efficient sortByKey() for now
    //val vertexSampleArray = weightedVertexRdd.top(size)(Ordering.by { case (vertexWeight, vertex) => vertexWeight })

    val sortedBySampleWeightRdd = weightedVertexRdd.sortByKey(ascending = false)
    val vertexSampleArray = sortedBySampleWeightRdd.take(size)

    val vertexSamplePairRdd = weightedVertexRdd.sparkContext.parallelize(vertexSampleArray)

    vertexSamplePairRdd.map { case (vertexWeight, vertex) => vertex }
  }

  /**
   * Add the degree histogram bin size for each vertex as the degree weight
   *
   * @param vertices RDD of all vertices
   * @param edges RDD of all edges
   * @return RDD of tuples that contain vertex degree histogram bin size as weight for each vertex
   */
  def addVertexDegreeDistWeights(vertices: RDD[GBVertex], edges: RDD[GBEdge]): RDD[(Long, GBVertex)] = {
    // get tuples of (vertexDegree, vertex)
    val vertexDegreeRdd = addVertexDegreeWeights(vertices, edges)

    // get tuples of (vertexDegreeBinSize, vertex)
    val groupedByBinSizeRdd = vertexDegreeRdd.groupBy { case (vertexDegreeBinSize, vertex) => vertexDegreeBinSize }
    val degreeDistRdd = groupedByBinSizeRdd.map { case (vertexDegreeBinSize, vertexSeq) => (vertexDegreeBinSize, vertexSeq.size.toLong) }

    degreeDistRdd.join(vertexDegreeRdd).map { case (vertexDegreeBinSize, degreeVertexPair) => degreeVertexPair }
  }

  /**
   * Add the out-degree for each vertex as the degree weight
   *
   * @param vertices RDD of all vertices
   * @param edges RDD of all edges
   * @return RDD of tuples that contain vertex degree as weight for each vertex
   */
  def addVertexDegreeWeights(vertices: RDD[GBVertex], edges: RDD[GBEdge]): RDD[(Long, GBVertex)] = {
    val vertexDegreePairs: RDD[(GBVertex, Long)] = UnweightedDegrees.outDegrees(vertices, edges)
    vertexDegreePairs.map({ case (vertex, degree) => (degree, vertex) })
  }
}
