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

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient._
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.plugins.orientdb.OrientDbTypeConverter
import scala.collection.mutable.ListBuffer

/**
 * imports vertices and edges schemas from OrientDB to ATK
 *
 * @param graph OrientDB graph database
 */
class SchemaReader(graph: OrientGraphNoTx) extends EventLogging {

  /**
   * A method imports vertex schema from OrientDB to ATK vertex schema
   *
   * @return ATK vertex schema
   */
  def importVertexSchema(className: String): VertexSchema = {

    try {
      createVertexSchema(className)
    }
    catch {
      case e: Exception =>
        error("Unable to read vertex schema from OrientDB graph", exception = e)
        throw new RuntimeException(s"Unable to read vertex schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates ATK vertex schema
   *
   * @param className OrientDB vertex class name
   * @return ATK vertex schema
   */
  def createVertexSchema(className: String): VertexSchema = {
    var columns = new ListBuffer[Column]()
    val propKeysIterator = graph.getRawGraph.getMetadata.getSchema.getClass(className).properties().iterator()
    while (propKeysIterator.hasNext) {
      val prop = propKeysIterator.next()
      val propKey = prop.getName
      val propType = prop.getType
      val columnType = OrientDbTypeConverter.convertOrientDbtoDataType(propType)
      val newColumn = new Column(propKey, columnType)
      columns += newColumn
    }
    columns += Column(GraphSchema.labelProperty, DataTypes.str)
    VertexSchema(columns.toList, className)
  }

  /**
   * A method imports edge schema from OrientDB to ATK edge schema
   *
   * @return ATK edge schema
   */
  def importEdgeSchema(className: String): EdgeSchema = {
    try {
      createEdgeSchema(className)
    }
    catch {
      case e: Exception =>
        error("Unable to read edge schema from OrientDB graph", exception = e)
        throw new RuntimeException(s"Unable to read edge schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates ATK edge schema
   *
   * @param className OrientDB edge class name
   * @return ATK edge schema
   */
  def createEdgeSchema(className: String): EdgeSchema = {

    var columns = new ListBuffer[Column]()
    val propKeysIterator = graph.getRawGraph.getMetadata.getSchema.getClass(className).properties().iterator()
    while (propKeysIterator.hasNext) {
      val prop = propKeysIterator.next()
      val propKey = prop.getName
      val propType = prop.getType
      val columnType = OrientDbTypeConverter.convertOrientDbtoDataType(propType)
      val newColumn = new Column(propKey, columnType)
      columns += newColumn
    }
    columns += Column(GraphSchema.labelProperty, DataTypes.str)
    val srcVertexLabel = graph.getEdgesOfClass(className).iterator().next().getVertex(Direction.OUT).asInstanceOf[OrientVertex].getLabel
    val destVertexLabel = graph.getEdgesOfClass(className).iterator().next().getVertex(Direction.IN).asInstanceOf[OrientVertex].getLabel
    EdgeSchema(columns.toList, className, srcVertexLabel, destVertexLabel)
  }

}
