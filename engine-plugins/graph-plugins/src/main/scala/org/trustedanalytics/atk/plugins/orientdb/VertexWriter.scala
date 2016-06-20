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
package org.trustedanalytics.atk.plugins.orientdb

import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.{ OrientDynaElementIterable, OrientGraphNoTx }
import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }
import org.apache.spark.atk.graph.Vertex
import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Export vertex to OrientDB vertex
 *
 * @param orientGraph an instance of Orient graph database
 */
class VertexWriter(orientGraph: OrientGraphNoTx) {

  require(orientGraph != null, "The OrientDB graph database instance must not equal null")

  /**
   * Method for creates a vertex
   *
   * @param vertex atk vertex to be converted to OrientDB BlueprintsVertex
   * @return OrientDB BlueprintsVertex
   */
  def create(vertex: Vertex): BlueprintsVertex = {

    val className: String = vertex.schema.label
    val orientVertexType: BlueprintsVertex = orientGraph.addVertex(className, null)
    val rowWrapper = new RowWrapper(vertex.schema)
    vertex.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        orientVertexType.setProperty(col.name, rowWrapper(vertex.row).value(col.name))
      }
    })
    orientVertexType
  }

  /**
   * a method for looking up a vertex in OrientDB graph and creates a new vertex if not found
   *
   * @param vertexId the vertex ID
   * @return OrientDB vertex
   */
  def findOrCreate(vertexId: Long, className: String): BlueprintsVertex = {
    val vertex = find(vertexId, className)
    vertex match {
      case Some(vertex) => vertex
      case _ =>
        val newVertex = orientGraph.addVertex(className, null)
        newVertex.setProperty(GraphSchema.vidProperty, vertexId)
        newVertex
    }
  }

  /**
   * a method that finds a vertex
   *
   * @param vertexId vertex ID
   * @return  OrientDB vertex if exists or null if not found
   */
  def find(vertexId: Long, className: String): Option[BlueprintsVertex] = {
    val vertices: OrientDynaElementIterable = orientGraph.command(
      new OCommandSQL(s"select from ${className} where ${GraphSchema.vidProperty} = ${vertexId}")
    ).execute()
    val vertexIterator = vertices.iterator().asInstanceOf[java.util.Iterator[BlueprintsVertex]]
    if (vertexIterator.hasNext) {
      val existingVertex = vertexIterator.next()
      return Some(existingVertex)
    }
    None
  }

  /**
   * updates an existing OrientDB vertex
   *
   * @param vertex ATK vertex
   * @param orientDbVertex OrientDB vertex
   * @return updated OrientDB vertex
   */
  def update(vertex: Vertex, orientDbVertex: BlueprintsVertex): BlueprintsVertex = {
    val rowWrapper = new RowWrapper(vertex.schema)
    vertex.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        orientDbVertex.setProperty(col.name, rowWrapper(vertex.row).value(col.name))
      }
    })
    orientDbVertex
  }

  /**
   * a method that updates OrientDB vertex if exists or creates a new vertex if not found
   *
   * @param vertex ATK vertex
   * @return OrientDB vertex
   */
  def updateOrCreate(vertex: Vertex): BlueprintsVertex = {
    val orientVertex = find(vertex.vid, vertex.label)
    val newVertex = if (orientVertex.isEmpty) {
      create(vertex)
    }
    else {
      update(vertex, orientVertex.get)
    }
    newVertex
  }
}
