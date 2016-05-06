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

import com.tinkerpop.blueprints.{ Parameter, Vertex => BlueprintsVertex }
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphNoTx, OrientEdgeType, OrientVertexType }
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, EdgeSchema, VertexSchema }

/**
 * Export schema to OrientDB schema
 *
 * @param orientGraph OrientDB graph
 */
class SchemaWriter(orientGraph: OrientGraphNoTx) {

  /**
   * A method to export vertex schema
   *
   * @param vertexSchema atk vertex schema
   * @return OrientDB vertex type
   */
  def createVertexSchema(vertexSchema: VertexSchema): OrientVertexType = {
    val vColumns = vertexSchema.columns
    val className: String = vertexSchema.label
    try {
      val orientVertexType = orientGraph.createVertexType(className)
      vColumns.foreach(col => {
        if (col.name != GraphSchema.labelProperty) {
          val orientColumnDataType = OrientDbTypeConverter.convertDataTypeToOrientDbType(col.dataType)
          orientVertexType.createProperty(col.name, orientColumnDataType)
        }
      })
      orientGraph.createKeyIndex(GraphSchema.vidProperty, classOf[BlueprintsVertex], new Parameter("class", className), new Parameter("type", "UNIQUE"))
      orientVertexType
    }
    catch {
      case e: Exception => {
        orientGraph.rollback()
        throw new RuntimeException(s"Unable to create the vertex schema: ${e.getMessage}")
      }
    }
  }

  /**
   * A method to export the edge schema
   *
   * @param edgeSchema ATK edge schema
   * @return OrientDB edge type
   */
  def createEdgeSchema(edgeSchema: EdgeSchema): OrientEdgeType = {
    val className: String = edgeSchema.label
    try {
      val oEdgeType = orientGraph.createEdgeType(className)
      val eColumns = edgeSchema.columns
      eColumns.foreach(col => {
        if (col.name != GraphSchema.labelProperty) {
          val orientColumnDataType = OrientDbTypeConverter.convertDataTypeToOrientDbType(col.dataType)
          oEdgeType.createProperty(col.name, orientColumnDataType)
        }
      })
      oEdgeType
    }
    catch {
      case e: Exception => {
        orientGraph.rollback()
        throw new RuntimeException(s"Unable to create the edge schema: ${e.getMessage}")
      }
    }
  }

}
