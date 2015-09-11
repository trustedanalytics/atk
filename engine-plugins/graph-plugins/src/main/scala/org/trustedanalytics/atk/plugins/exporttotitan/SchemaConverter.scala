/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.plugins.exporttotitan

import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.graphbuilder.schema.{ EdgeLabelDef, PropertyType, PropertyDef, GraphSchema }
import org.trustedanalytics.atk.graphbuilder.util.PrimitiveConverter

/**
 * Convert schema of a seamless Graph to a Titan Schema
 */
object SchemaConverter {

  def convert(frameSchemas: List[GraphElementSchema]): GraphSchema = {

    val vertexPropDefs = frameSchemas.filter(_.isInstanceOf[VertexSchema]).flatMap(schema => {
      val vertexSchema = schema.asInstanceOf[VertexSchema]
      vertexSchema.columns.map(column => {
        val isIdColumn = vertexSchema.idColumnName match {
          case Some(name) => name.equals(column.name)
          case _ => false
        }
        new PropertyDef(PropertyType.Vertex, column.name, PrimitiveConverter.primitivesToObjects(column.dataType.scalaType), unique = isIdColumn, indexed = isIdColumn)
      })
    })

    val edgePropDefs = frameSchemas.filter(_.isInstanceOf[EdgeSchema]).flatMap(schema => {
      val edgeSchema = schema.asInstanceOf[EdgeSchema]
      edgeSchema.columns.map(column => {
        new PropertyDef(PropertyType.Edge, column.name, PrimitiveConverter.primitivesToObjects(column.dataType.scalaType))
      })
    })

    val edgeLabelDefs = frameSchemas.filter(_.isInstanceOf[EdgeSchema]).map(schema => EdgeLabelDef(schema.label))

    val propertyDefs = vertexPropDefs ++ edgePropDefs

    new GraphSchema(edgeLabelDefs, propertyDefs)
  }

}
