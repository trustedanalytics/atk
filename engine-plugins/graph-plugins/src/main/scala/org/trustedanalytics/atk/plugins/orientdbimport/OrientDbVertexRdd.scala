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
package org.trustedanalytics.atk.plugins.orientdbimport

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable
import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }
import org.apache.spark.atk.graph.Vertex
import org.apache.spark.rdd.RDD
import org.apache.spark.{ Partition, SparkContext, TaskContext }
import org.trustedanalytics.atk.plugins.orientdb.{ DbConfiguration, GraphDbFactory }
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @param sc
 * @param dbConfigurations
 */
class OrientDbVertexRdd(sc: SparkContext, dbConfigurations: DbConfiguration) extends RDD[Vertex](sc, Nil) {

  /**
   *
   * @param split
   * @param context
   * @return
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Vertex] = {
    val graph = GraphDbFactory.graphDbConnector(dbConfigurations)
    val partition = split.asInstanceOf[OrientDbPartition]
    val vertexBuffer = new ArrayBuffer[Vertex]()
    val schemaReader = new SchemaReader(graph)
    val vertexSchema = schemaReader.importVertexSchema(partition.className)
    val vertices: OrientDynaElementIterable = graph.command(
      new OCommandSQL(s"select from cluster:${partition.clusterId} where @class='${partition.className}'")
    ).execute()
    val vertexIterator = vertices.iterator().asInstanceOf[java.util.Iterator[BlueprintsVertex]]
    while (vertexIterator.hasNext) {
      val vertexReader = new VertexReader(graph, vertexSchema)
      val vertex = vertexReader.importVertex(vertexIterator.next())
      vertexBuffer += vertex
    }
    vertexBuffer.toIterator
  }

  /**
   *
   * @return
   */
  override protected def getPartitions: Array[Partition] = {
    val partitionBuffer = new ArrayBuffer[OrientDbPartition]()
    val graph = GraphDbFactory.graphDbConnector(dbConfigurations)
    val classBaseNames = graph.getVertexBaseType.getName
    val classIterator = graph.getVertexType(classBaseNames).getAllSubclasses.iterator()
    var paritionIdx = 0
    while (classIterator.hasNext) {
      val classLabel = classIterator.next().getName
      val clusterIds = graph.getVertexType(classLabel).getClusterIds
      clusterIds.foreach(id => {
        partitionBuffer += new OrientDbPartition(id, classLabel, paritionIdx)
        paritionIdx += 1
      })
    }
    partitionBuffer.toArray
  }
}
