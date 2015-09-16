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

package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.JoinBroadcastVariable
import org.trustedanalytics.atk.graphbuilder.elements.{ GbIdToPhysicalId, GBEdge }
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.JoinBroadcastVariable
import org.trustedanalytics.atk.graphbuilder.elements._
import org.trustedanalytics.atk.graphbuilder.graph.titan.TitanGraphConnector
import org.trustedanalytics.atk.graphbuilder.write.EdgeWriter
import org.trustedanalytics.atk.graphbuilder.write.dao.{ EdgeDAO, VertexDAO }
import org.apache.spark.SparkContext._
import org.apache.spark.{ RangePartitioner, TaskContext }
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * Functions that are applicable to Edge RDD's
 *
 * This is best used by importing GraphBuilderRDDImplicits._
 *
 * @param self input that these functions are applicable to
 * @param maxEdgesPerCommit Titan performs poorly if you try to commit edges in too
 *                          large of batches.  With Vertices the limit is much lower (10k).
 *                          The limit for Edges is much higher but there is still a limit.
 *                          It is hard to tell what the right number is for this one.
 *                          I think somewhere larger than 400k is getting too big.
 */
class EdgeRddFunctions(self: RDD[GBEdge], val maxEdgesPerCommit: Long = 10000L) extends Serializable {

  /**
   * Merge duplicate Edges, creating a new Edge that has a combined set of properties.
   *
   * @return an RDD without duplicates
   */
  def mergeDuplicates(): RDD[GBEdge] = {
    self.groupBy(m => m.id).mapValues(dups => dups.reduce((m1, m2) => m1.merge(m2))).values
  }

  /**
   * For every Edge, create an additional Edge going in the opposite direction. This
   * method will double the size of the RDD.
   * <p>
   * Depending on the source data, duplicates may be created in this process. So you may
   * need to merge duplicates after.
   * </p>
   */
  def biDirectional(): RDD[GBEdge] = {
    self.flatMap(edge => List(edge, edge.reverse()))
  }

  /**
   * For every Edge, output its head Vertex GbId
   */
  def headVerticesGbIds(): RDD[Property] = {
    self.map(edge => edge.headVertexGbId)
  }

  /**
   * For every Edge, output its tail Vertex GbId
   */
  def tailVerticesGbIds(): RDD[Property] = {
    self.map(edge => edge.tailVertexGbId)
  }

  /**
   * For every Edge, create two vertices, one from the tail Vertex GbId, and one from the
   * head Vertex GbId.
   * <p>
   * This functionality was called "retain dangling edges" in GB version 2.
   * </p>
   */
  def verticesFromEdges(): RDD[GBVertex] = {
    self.flatMap(edge => List(new GBVertex(edge.tailVertexGbId, Set.empty[Property]), new GBVertex(edge.headVertexGbId, Set.empty[Property])))
  }

  /**
   * Join Edges with the Physical Id's so they won't need to be looked up when writing Edges.
   *
   * This is an inner join.  Edges that can't be joined are dropped.
   */
  def joinWithPhysicalIds(ids: RDD[GbIdToPhysicalId]): RDD[GBEdge] = {

    val idsByGbId = ids.map(idMap => (idMap.gbId, idMap))

    // set physical ids for tail (source) vertices
    val edgesByTail = self.map(edge => (edge.tailVertexGbId, edge))

    // Range-partitioner helps alleviate the "com.esotericsoftware.kryo.KryoException: java.lang.NegativeArraySizeException"
    // which occurs when we spill blocks to disk larger than 2GB but does not completely fix the problem
    // TODO: Find a better way to handle supernodes: Look at skewed joins in Pig
    implicit val propertyOrdering = PropertyOrdering // Ordering is needed by Spark's range partitioner
    val tailPartitioner = new RangePartitioner(edgesByTail.partitions.length, edgesByTail)
    val edgesWithTail = edgesByTail.join(idsByGbId, tailPartitioner).map {
      case (gbId, (edge, gbIdToPhysicalId)) =>
        val physicalId = gbIdToPhysicalId.physicalId
        (edge.headVertexGbId, edge.copy(tailPhysicalId = physicalId))
    }

    // set physical ids for head (destination) vertices
    val edgesWithPhysicalIds = edgesWithTail.join(idsByGbId).map {
      case (gbId, (edge, gbIdToPhysicalId)) => {
        val physicalId = gbIdToPhysicalId.physicalId
        edge.copy(headPhysicalId = physicalId)
      }
    }

    edgesWithPhysicalIds
  }

  /**
   * Filter Edges that do NOT have physical ids
   */
  def filterEdgesWithoutPhysicalIds(): RDD[GBEdge] = {
    self.filter(edge => {
      if (edge.tailPhysicalId == null || edge.headPhysicalId == null) false
      else true
    })
  }

  /**
   * Write the Edges to Titan using the supplied connector
   * @param append true to append to an existing graph
   */
  def write(titanConnector: TitanGraphConnector, append: Boolean): Unit = {

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[GBEdge]) => {

      val graph = TitanGraphConnector.getGraphFromCache(titanConnector)
      val edgeDAO = new EdgeDAO(graph, new VertexDAO(graph))
      val writer = new EdgeWriter(edgeDAO, append)

      try {
        var count = 0L
        while (iterator.hasNext) {
          writer.write(iterator.next())
          count += 1
          if (count % maxEdgesPerCommit == 0) {
            graph.commit()
          }
        }

        println("wrote edges: " + count + " for split: " + context.partitionId)

        graph.commit()
      }
      finally {
        //Do not shut down graph when using cache since graph instances are automatically shutdown when
        //no more references are held
        //graph.shutdown()
      }
    })
  }

  /**
   * Write the Edges to Titan using the supplied connector
   * @param append true to append to an existing graph
   * @param gbIdToPhysicalIdMap see GraphBuilderConfig.broadcastVertexIds
   */
  def write(titanConnector: TitanGraphConnector, gbIdToPhysicalIdMap: JoinBroadcastVariable[Property, AnyRef], append: Boolean): Unit = {

    self.context.runJob(self, (context: TaskContext, iterator: Iterator[GBEdge]) => {
      val graph = TitanGraphConnector.getGraphFromCache(titanConnector)
      val edgeDAO = new EdgeDAO(graph, new VertexDAO(graph))
      val writer = new EdgeWriter(edgeDAO, append)

      try {
        var count = 0L
        while (iterator.hasNext) {
          val edge = iterator.next()
          edge.tailPhysicalId = gbIdToPhysicalIdMap(edge.tailVertexGbId)
          edge.headPhysicalId = gbIdToPhysicalIdMap(edge.headVertexGbId)
          writer.write(edge)
          count += 1
          if (count % maxEdgesPerCommit == 0) {
            graph.commit()
          }
        }

        println("wrote edges: " + count + " for split: " + context.partitionId)

        graph.commit()
      }
      finally {
        //Do not shut down graph when using cache since graph instances are automatically shutdown when
        //no more references are held
        //graph.shutdown()
      }
    })
  }
}
