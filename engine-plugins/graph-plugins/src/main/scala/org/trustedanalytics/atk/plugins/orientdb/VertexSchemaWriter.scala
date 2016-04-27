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
import com.tinkerpop.blueprints.impls.orient.{ OrientVertexType, OrientGraph }
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, VertexSchema }

/**
 * Export vertex schema to OrientDB
 */
class VertexSchemaWriter {

  /**
   * Method to export the vertex schema
   *
   * @param orientGraph  an instance of Orient graph database
   * @param vertexSchema ATK vertex schema
   * @return Orient vertex schema
   */

  def createVertexSchema(orientGraph: OrientGraph, vertexSchema: VertexSchema): OrientVertexType = {

    val vColumns = vertexSchema.columns
    val className: String = vertexSchema.label
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
}
