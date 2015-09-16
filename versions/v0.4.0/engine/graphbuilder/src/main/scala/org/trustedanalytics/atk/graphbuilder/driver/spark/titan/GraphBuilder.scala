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

package org.trustedanalytics.atk.graphbuilder.driver.spark.titan

import java.text.NumberFormat

import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.graph.titan.{ TitanGraphCacheListener, TitanGraphConnector }
import org.trustedanalytics.atk.graphbuilder.parser.rule._
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.trustedanalytics.atk.graphbuilder.schema.GraphSchema

/**
 * This is a GraphBuilder that runs on Spark, uses a RuleParser and creates Graphs in Titan.
 * <p>
 * This class wraps the Spark data flow and gives an example of how you can compose a
 * Graph Building utility from the other classes.
 * </p>
 *
 * @param config configuration options
 */
class GraphBuilder(config: GraphBuilderConfig) extends Serializable {

  val titanConnector = new TitanGraphConnector(config.titanConfig)
  val titanSchemaManager = new InferSchemaManager(config)
  val vertexParser = new VertexRuleParser(config.inputSchema.serializableCopy, config.vertexRules)
  val edgeParser = new EdgeRuleParser(config.inputSchema.serializableCopy, config.edgeRules)

  /**
   * Build the Graph, both Edges and Vertices from one source.
   *
   * @param inputRdd the input rows to create the graph from
   */
  def build(inputRdd: RDD[Seq[_]]) {
    build(inputRdd, inputRdd)
  }

  /**
   * Build the Graph, separate sources for Edges and Vertices
   *
   * @param vertexInputRdd the input rows to create the vertices from
   * @param edgeInputRdd the input rows to create the edges from
   */
  def build(vertexInputRdd: RDD[Seq[_]], edgeInputRdd: RDD[Seq[_]]) {
    if (config.inferSchema) {
      titanSchemaManager.writeSchemaFromRules()
    }

    println("Parse and Write Vertices")
    val vertices = vertexInputRdd.parseVertices(vertexParser)
    val edges = edgeInputRdd.parseEdges(edgeParser)

    buildGraphWithSpark(vertices, edges)
  }

  /**
   * Build the Graph using Spark
   *
   * @param vertexRdd RDD of Vertex objects
   * @param edgeRdd RDD of Edge objects
   */
  def buildGraphWithSpark(vertexRdd: RDD[GBVertex], edgeRdd: RDD[GBEdge]) {

    var vertices = vertexRdd
    var edges = edgeRdd

    // Adding listener that evicts Titan graph instances from cache when it the application ends
    // Needed to shutdown run-away Titan threads if the application ends abruptly
    // https://github.com/thinkaurelius/titan/issues/817
    vertexRdd.sparkContext.addSparkListener(new TitanGraphCacheListener())

    if (config.retainDanglingEdges) {
      println("retain dangling edges was true so we'll create extra vertices from edges")
      vertices = vertices.union(edges.verticesFromEdges())
    }

    if (config.inferSchema && titanSchemaManager.needsToInferSchemaFromData) {
      println("inferring schema from data")
      titanSchemaManager.writeSchemaFromData(edges, vertices)
    }

    val mergedVertices = vertices.mergeDuplicates()
    val idMap = mergedVertices.write(titanConnector, config.append)
    idMap.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("done parsing and writing, vertices count: " + NumberFormat.getInstance().format(idMap.count()))

    val broadcastJoinThreshold = titanConnector.config.getLong("auto-partitioner.broadcast-join-threshold", 0)

    if (config.broadcastVertexIds || JoinBroadcastVariable.useBroadcastVariable(idMap, broadcastJoinThreshold)) {
      val vertexMap = idMap.map(gbIdToPhysicalId => gbIdToPhysicalId.toTuple)

      println("broadcasting vertex ids")
      val vertexMapSize = JoinBroadcastVariable
      val gbIdToPhysicalIdMap = JoinBroadcastVariable(vertexMap)

      println("starting write of edges")
      edges.write(titanConnector, gbIdToPhysicalIdMap, config.append)
    }
    else {
      println("join edges with physical ids")
      val edgesWithPhysicalIds = edges.joinWithPhysicalIds(idMap)

      println("starting write of edges")
      edgesWithPhysicalIds.write(titanConnector, config.append)
    }

    // Manually Unpersist RDDs to help with Memory usage
    idMap.unpersist(blocking = false)

    TitanGraphConnector.invalidateGraphCache()

    println("done writing edges")
  }

}
