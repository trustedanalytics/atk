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

package org.trustedanalytics.atk.engine.graph.plugins.exportfromtitan

import org.trustedanalytics.atk.domain.schema
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex

import scala.collection.immutable.Map

/**
 * Aggregation methods for getting VertexSchema from GBVertices
 *
 * @param indexNames vertex properties with unique indexes in Titan
 */
class VertexSchemaAggregator(indexNames: List[String]) extends Serializable {

  def toSchema(vertex: GBVertex): VertexSchema = {
    val columns = vertex.properties.map(property => Column(property.key, DataTypes.dataTypeOfValue(property.value)))

    val columnNames = vertex.properties.map(_.key)
    val indexedProperties = indexNames.intersect(columnNames.toSeq)
    val userDefinedColumn = if (indexedProperties.isEmpty) Some("titanPhysicalId") else Some(indexedProperties.head)
    val label = vertex.getProperty(GraphSchema.labelProperty).get.value.toString

    val allColumns = schema.GraphSchema.vertexSystemColumns ++ columns.filterNot(col => GraphSchema.vertexSystemColumnNamesSet.contains(col.name))

    new VertexSchema(allColumns.toList, label, userDefinedColumn)
  }

  val zeroValue = Map[String, VertexSchema]()

  def seqOp(vertexSchemas: Map[String, VertexSchema], vertex: GBVertex): Map[String, VertexSchema] = {
    val schema = toSchema(vertex)
    val label = schema.label
    if (vertexSchemas.contains(label)) {
      val schemaCombined = vertexSchemas.get(label).get.union(schema).asInstanceOf[VertexSchema]
      vertexSchemas + (label -> schemaCombined)
    }
    else {
      vertexSchemas + (label -> schema)
    }
  }

  def combOp(vertexSchemasA: Map[String, VertexSchema], vertexSchemasB: Map[String, VertexSchema]): Map[String, VertexSchema] = {
    val combinedKeys = vertexSchemasA.keySet ++ vertexSchemasB.keySet
    val combined = combinedKeys.map(key => {
      val valueA = vertexSchemasA.get(key)
      val valueB = vertexSchemasB.get(key)
      if (valueA.isDefined) {
        if (valueB.isDefined) {
          key -> valueA.get.union(valueB.get).asInstanceOf[VertexSchema]
        }
        else {
          key -> valueA.get
        }
      }
      else {
        key -> valueB.get
      }
    })
    combined.toMap
  }

}
