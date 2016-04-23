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

import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }
import org.apache.spark.atk.graph.Vertex
import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Export vertex to OrientDB vertex
 *
 * @param oGraph an instance of Orient graph database
 */
class VertexWriter(oGraph: OrientGraph) {

  /**
   * Method for exporting a vertex
   *
   * @param vertex atk vertex to be converted to Orient BlueprintsVertex
   * @return Orient BlueprintsVertex
   */

  def addVertex(vertex: Vertex): BlueprintsVertex = {

    require(oGraph != null, "The Orient graph database instance must not equal null")
    val className: String = vertex.schema.label
    if (oGraph.getVertexType(className) == null) {
      val createVertexSchema = new VertexSchemaWriter
      val oVertexType = createVertexSchema.createVertexSchema(oGraph, vertex.schema)
    }
    val oVertex: BlueprintsVertex = oGraph.addVertex(className, null)
    val rowWrapper = new RowWrapper(vertex.schema)
    vertex.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        oVertex.setProperty(col.name, rowWrapper(vertex.row).value(col.name))
      }
    })

    oVertex

  }

  /**
   * a method for checking an existing vertex and creates a new vertex if not found
   * @param vertexId the vertex ID
   * @return vertex
   */
  def findOrCreateVertex(vertexId: Long): BlueprintsVertex = {

    val vertexIterator = oGraph.getVertices(GraphSchema.vidProperty, vertexId).iterator()
    if (vertexIterator.hasNext) {
      val existingVertex = vertexIterator.next()
      existingVertex
    }
    else {
      val newVertex = oGraph.addVertex(GraphSchema.labelProperty, null)
      newVertex.setProperty(GraphSchema.vidProperty, vertexId)
      newVertex
    }

  }
}
