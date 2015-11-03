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

package org.trustedanalytics.atk.graphbuilder.schema

import org.apache.spark.AccumulableParam
import org.trustedanalytics.atk.graphbuilder.elements.{ GraphElement, GBVertex, GBEdge }

/**
 * Implements AccumulableParam to allow inferring graph schema from data.
 */
class SchemaAccumulableParam extends AccumulableParam[InferSchemaFromData, GraphElement] {

  /**
   * Add a new edge or vertex to infer schema object accumulator
   *
   * @param schema the current infer schema object
   * @param element the graph element to add to the current infer schema object
   * @return
   */
  override def addAccumulator(schema: InferSchemaFromData, element: GraphElement): InferSchemaFromData = {
    element match {
      case v: GBVertex => schema.add(v)
      case e: GBEdge => schema.add(e)
    }
    schema
  }

  /**
   * Merges two accumulated graph schemas (as InferSchemaFromData) together
   *
   * @param schemaOne accumulated infer schema object one
   * @param schemaTwo accumulated infer schema object two
   * @return the merged infer schema object
   */
  override def addInPlace(schemaOne: InferSchemaFromData, schemaTwo: InferSchemaFromData): InferSchemaFromData = {
    schemaOne.edgeLabelDefsMap ++= schemaTwo.edgeLabelDefsMap
    schemaOne.propertyDefsMap ++= schemaTwo.propertyDefsMap
    schemaOne
  }

  override def zero(initialValue: InferSchemaFromData): InferSchemaFromData = {
    initialValue
  }

}
