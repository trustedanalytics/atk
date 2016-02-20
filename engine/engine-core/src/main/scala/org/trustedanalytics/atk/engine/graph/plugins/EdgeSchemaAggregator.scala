/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.graph.plugins

import org.trustedanalytics.atk.domain.schema._

/**
 * Aggregation methods for getting EdgeSchema from GBEdges
 */
object EdgeSchemaAggregator extends Serializable {

  def toSchema(edgeHolder: EdgeHolder): EdgeSchema = {
    val columns = edgeHolder.edge.properties.map(property => Column(property.key, DataTypes.dataTypeOfValue(property.value)))
    val columnNames = edgeHolder.edge.properties.map(_.key)
    val allColumns = GraphSchema.edgeSystemColumns ++ columns.filterNot(col => GraphSchema.edgeSystemColumnNamesSet.contains(col.name))

    new EdgeSchema(allColumns.toList, edgeHolder.edge.label, edgeHolder.srcLabel, edgeHolder.destLabel, directed = true)
  }

  val zeroValue = Map[String, EdgeSchema]()

  def seqOp(edgeSchemas: Map[String, EdgeSchema], edgeHolder: EdgeHolder): Map[String, EdgeSchema] = {
    val schema = toSchema(edgeHolder)
    val label = schema.label
    if (edgeSchemas.contains(label)) {
      val schemaCombined = edgeSchemas.get(label).get.union(schema).asInstanceOf[EdgeSchema]
      edgeSchemas + (label -> schemaCombined)
    }
    else {
      edgeSchemas + (label -> schema)
    }
  }

  def combOp(edgeSchemasA: Map[String, EdgeSchema], edgeSchemasB: Map[String, EdgeSchema]): Map[String, EdgeSchema] = {
    val combinedKeys = edgeSchemasA.keySet ++ edgeSchemasB.keySet
    val combined = combinedKeys.map(key => {
      val valueA = edgeSchemasA.get(key)
      val valueB = edgeSchemasB.get(key)
      if (valueA.isDefined) {
        if (valueB.isDefined) {
          key -> valueA.get.union(valueB.get).asInstanceOf[EdgeSchema]
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
