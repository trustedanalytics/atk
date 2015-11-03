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


package org.trustedanalytics.atk.engine.graph

import org.trustedanalytics.atk.domain.StorageFormats
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphReference }
import org.trustedanalytics.atk.domain.schema.{ EdgeSchema, VertexSchema }
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }
import org.apache.spark.SparkContext
import org.apache.spark.atk.graph.{ EdgeFrameRdd, VertexFrameRdd }
import org.apache.spark.rdd.RDD

/**
 * Interface for working with Graphs for plugin authors
 */
trait Graph {

  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  def entity: GraphEntity

  def isSeamless: Boolean

  def isTitan: Boolean

  def nextId: Long

  def incrementIdCounter(value: Long): Unit

  // TODO: not sure to leave these here or move to a SeamlessGraph interface

  def defineVertexType(vertexSchema: VertexSchema): Unit

  def defineEdgeType(edgeSchema: EdgeSchema): Unit

}

object Graph {

  implicit def graphToGraphEntity(graph: Graph): GraphEntity = graph.entity

  implicit def graphToGraphReference(graph: Graph): GraphReference = graph.entity.toReference
}

/**
 * Interface for working with Graphs for plugin authors, with Spark support
 */
trait SparkGraph extends Graph {

  /**
   * Pair of RDDs representing the Graph
   */
  def gbRdds: (RDD[GBVertex], RDD[GBEdge])

  def gbVertexRdd: RDD[GBVertex]

  def gbEdgeRdd: RDD[GBEdge]

  def vertexRdd(label: String): VertexFrameRdd

  def edgeRdd(label: String): EdgeFrameRdd

  // TODO: writeToTitan()

}

object SparkGraph {

  implicit def sparkGraphToGraphEntity(graph: SparkGraph): GraphEntity = graph.entity

  implicit def sparkGraphToGraphReference(graph: SparkGraph): GraphReference = graph.entity.toReference
}

class GraphImpl(graph: GraphReference, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends Graph {

  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  override def entity: GraphEntity = {
    sparkGraphStorage.expectGraph(graph)
  }

  override def isSeamless: Boolean = entity.isSeamless

  override def isTitan: Boolean = entity.isTitan

  override def nextId: Long = entity.nextId()

  override def incrementIdCounter(value: Long): Unit = sparkGraphStorage.incrementIdCounter(graph, value)

  override def defineVertexType(vertexSchema: VertexSchema): Unit = {
    sparkGraphStorage.defineVertexType(graph, vertexSchema)
  }

  override def defineEdgeType(edgeSchema: EdgeSchema): Unit = {
    sparkGraphStorage.defineEdgeType(graph, edgeSchema)
  }
}

class SparkGraphImpl(graph: GraphReference, sc: SparkContext, sparkGraphStorage: SparkGraphStorage)(implicit invocation: Invocation)
    extends GraphImpl(graph, sparkGraphStorage)(invocation)
    with SparkGraph {

  /**
   * Pair of RDDs representing the Graph
   */
  override def gbRdds: (RDD[GBVertex], RDD[GBEdge]) = {
    sparkGraphStorage.loadGbElements(sc, entity)
  }

  override def gbVertexRdd: RDD[GBVertex] = {
    sparkGraphStorage.loadGbVertices(sc, entity)
  }

  override def gbEdgeRdd: RDD[GBEdge] = {
    sparkGraphStorage.loadGbEdges(sc, entity)
  }

  override def edgeRdd(label: String): EdgeFrameRdd = {
    val seamlessGraph = sparkGraphStorage.expectSeamless(graph)
    sparkGraphStorage.loadEdgeRDD(sc, seamlessGraph.edgeMeta(label))
  }

  override def vertexRdd(label: String): VertexFrameRdd = {
    val seamlessGraph = sparkGraphStorage.expectSeamless(graph)
    sparkGraphStorage.loadVertexRDD(sc, seamlessGraph.vertexMeta(label))
  }
}
