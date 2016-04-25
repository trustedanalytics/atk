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

import com.tinkerpop.blueprints.impls.orient.{ OrientEdgeType, OrientGraph }
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, EdgeSchema }

/**
 * export the edge schema
 * @param edgeSchema ATK edge schema
 */
class EdgeSchemaWriter(edgeSchema: EdgeSchema) {

  /**
   * Method to export the edge schema
   *
   * @param orientGraph  an instance of Orient graph database
   * @return Orient edge schema
   */
  def createEdgeSchema(orientGraph: OrientGraph): OrientEdgeType = {

    val className: String = edgeSchema.label
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
}
