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

import java.util
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient._
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.plugins.orientdb.OrientDbTypeConverter
import scala.collection.mutable.ListBuffer

/**
 * imports vertices and edges schemas from OrientDB to ATK
 *
 * @param graph OrientDB graph database
 */
class SchemaReader(graph: OrientGraphNoTx) {

  /**
   * A method imports vertex schema from OrientDB to ATK vertex schema
   * @return ATK vertex schema
   */
  def importVertexSchema(): VertexSchema = {

    try {
      val vertexTypeName = graph.getVertexBaseType.asInstanceOf[OrientVertexType].getName
      val className = graph.getVerticesOfClass(vertexTypeName).iterator().next().asInstanceOf[OrientVertex].getLabel
      val vertexPropertiesKeys = graph.getVerticesOfClass(className).iterator().next().asInstanceOf[OrientVertex].getProperties
      createVertexSchema(className, vertexPropertiesKeys)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates ATK vertex schema
   * @param className OrientDB vertex class name
   * @param propertiesKeys OrientDB vertex properties keys
   * @return ATK vertex schema
   */
  def createVertexSchema(className: String, propertiesKeys: util.Map[String, AnyRef]): VertexSchema = {
    var columns = new ListBuffer[Column]()
    val propKeys = graph.getVerticesOfClass(className).iterator().next().asInstanceOf[OrientVertex].getPropertyKeys
    while (propKeys.iterator().hasNext) {
      val propKey = propKeys.iterator.next()
      val propValue = propertiesKeys.get(propKey)
      val propOrientType = OType.getTypeByValue(propValue)
      val columnType = OrientDbTypeConverter.convertOrientDbtoDataType(propOrientType)
      val newColumn = new Column(propKey, columnType)
      columns += newColumn
      propKeys.remove(propKey)
    }
    columns += Column(GraphSchema.labelProperty, DataTypes.str)
    VertexSchema(columns.toList, className)
  }

  /**
   * A method imports edge schema from OrientDB to ATK edge schema
   * @return ATK edge schema
   */
  def importEdgeSchema(): EdgeSchema = {
    try {
      val edgeTypeName = graph.getEdgeBaseType.getName
      val edgeClassName = graph.getEdgesOfClass(edgeTypeName).iterator().next().asInstanceOf[OrientEdge].getLabel
      val properties = graph.getEdgesOfClass(edgeClassName).iterator().next().asInstanceOf[OrientEdge].getProperties
      createEdgeSchema(edgeClassName, properties)
    }
    catch {
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge schema from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates ATK edge schema
   * @param className OrientDB edge class name
   * @param properties OrientDB edge Properties keys
   * @return ATK edge schema
   */
  def createEdgeSchema(className: String, properties: util.Map[String, AnyRef]): EdgeSchema = {

    var columns = new ListBuffer[Column]()
    val propKeys = graph.getEdgesOfClass(className).iterator().next().asInstanceOf[OrientEdge].getPropertyKeys
    while (propKeys.iterator().hasNext) {
      val propKey = propKeys.iterator.next()
      val propValue = properties.get(propKey)
      val propOrientType = OType.getTypeByValue(propValue)
      val columnType = OrientDbTypeConverter.convertOrientDbtoDataType(propOrientType)
      val newColumn = new Column(propKey, columnType)
      columns += newColumn
      propKeys.remove(propKey)
    }
    columns += Column(GraphSchema.labelProperty, DataTypes.str)
    EdgeSchema(columns.toList, className, "source", "source")
  }

}
