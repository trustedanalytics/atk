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
 * Created by wtaie on 4/18/16.
 */
class EdgeSchemaWriter {

  /**
   * Method to export the edge schema
   * @param oGraph  an instance of Orient graph database
   * @param edgeSchema ATK edge schema
   * @return Orient edge schema
   */
  def createEdgeSchema(oGraph: OrientGraph, edgeSchema: EdgeSchema): OrientEdgeType = {

    val className: String = "Edge" + edgeSchema.label
    val oEdgeType = oGraph.createEdgeType(className)
    val eColumns = edgeSchema.columns
    eColumns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        val convertDataTypes = new DataTypeToOTypeConversion
        val oColumnDataType = convertDataTypes.convertDataTypeToOrientDbType(col.dataType)
        oEdgeType.createProperty(col.name, oColumnDataType)
      }
    })
    oEdgeType
  }
}
