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

package org.trustedanalytics.atk.plugins.clusteringcoefficient

import org.trustedanalytics.atk.graphbuilder.elements.GBVertex
import org.trustedanalytics.atk.domain.schema._

/**
 * Convert a GBVertex to a FrameSchema
 */
object FrameSchemaAggregator extends Serializable {

  def toSchema(vertex: GBVertex): FrameSchema = {
    val columns = vertex.properties.map(property => Column(property.key, DataTypes.dataTypeOfValue(property.value)))
    new FrameSchema(columns.toList)
  }

  val zeroValue: Schema = FrameSchema()

  def seqOp(schema: Schema, vertex: GBVertex): Schema = {
    schema.union(toSchema(vertex))
  }

  def combOp(schemaA: Schema, schemaB: Schema): Schema = {
    schemaA.union(schemaB)
  }
}
