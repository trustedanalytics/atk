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

import org.trustedanalytics.atk.graphbuilder.parser.rule.{ Value, EdgeRule, VertexRule, DataTypeResolver }
import org.trustedanalytics.atk.graphbuilder.util.StringUtils
import org.trustedanalytics.atk.graphbuilder.parser.rule._

/**
 * Infer Graph schema as best as possible from the supplied parameters.  This means if only part
 * of the schema can be inferred, it will infer that part and it is someone else's responsibility
 * to handle the rest.
 * <p>
 * This can only be done completely when property keys and Edge labels are defined as constants.
 * When property keys and/or Edge labels are defined to be dynamically parsed from the input then
 * this method will only provide the best it can. The user would either need to define the schema
 * up front or dynamically infer it from the InputRows.
 * </p>
 * @param dataTypeParser figures out the dataTypes from values and the inputSchema
 * @param vertexRules rules for how to parse vertices
 * @param edgeRules rules for how to parse edges
 */
class InferSchemaFromRules(dataTypeParser: DataTypeResolver, vertexRules: List[VertexRule], edgeRules: List[EdgeRule]) extends Serializable {

  /**
   * True if the entire schema can be inferred from the supplied inputSchema and rules, and both the vertexRules and edgeRules are nonempty
   */
  def canInferAll: Boolean = !(vertexRules.isEmpty && edgeRules.isEmpty) && canInferEdgeLabels && canInferAllPropertyKeyNames

  /**
   * True if all Edge labels are defined as constants in the rules
   * False if any Edge labels are to be dynamically parsed from the input
   */
  def canInferEdgeLabels: Boolean = {
    edgeRules.foreach(edgeRule =>
      if (edgeRule.label.isParsed) {
        return false
      })
    true
  }

  /**
   * True if every property can be inferred.
   * False if only part of the schema can be inferred.
   */
  def canInferAllPropertyKeyNames: Boolean = {

    // see if every property key name for Vertices is non-parsed
    vertexRules.foreach(_.fullPropertyRules.foreach(propertyRule =>
      if (propertyRule.key.isParsed) {
        return false
      }))

    // see if every property key name for Edges is non-parsed
    edgeRules.foreach(_.propertyRules.foreach(propertyRule =>
      if (propertyRule.key.isParsed) {
        return false
      }))

    true
  }

  /**
   * Infer the Graph schema from the InputSchema and parsing rules as best as possible.
   *
   * @return the schema to the extent it can be inferred.
   */
  def inferGraphSchema(): GraphSchema = {
    new GraphSchema(inferEdgeLabelDefs().distinct, distinctPropertyDefs(inferPropertyDefs()))
  }

  /**
   * Infer the EdgeLabelDefs as best as possible from the EdgeRules
   */
  private def inferEdgeLabelDefs(): List[EdgeLabelDef] = {
    for {
      edgeRule <- edgeRules
      if edgeRule.label.isNotParsed
    } yield new EdgeLabelDef(edgeRule.label.value)
  }

  /**
   * Distinct list of PropertyDefs by name
   */
  private def distinctPropertyDefs(list: List[PropertyDef]): List[PropertyDef] = {
    list.map(propertyDef => (propertyDef.name, propertyDef)).toMap.valuesIterator.toList
  }

  /**
   * Infer the PropertyDefs as best possible from the parsing rules and inputSchema
   */
  private def inferPropertyDefs(): List[PropertyDef] = {

    val vertexGbIdPropertyDefs = for {
      vertexRule <- vertexRules
      if vertexRule.gbId.key.isNotParsed
    } yield PropertyDef(PropertyType.Vertex, safeValue(vertexRule.gbId.key), dataTypeParser.get(vertexRule.gbId.value), unique = true, indexed = true)

    val vertexPropertyDefs = for {
      vertexRule <- vertexRules
      propertyRule <- vertexRule.propertyRules
      if propertyRule.key.isNotParsed
    } yield PropertyDef(PropertyType.Vertex, safeValue(propertyRule.key), dataTypeParser.get(propertyRule.value), unique = false, indexed = true)

    val edgePropertyDefs = for {
      edgeRule <- edgeRules
      propertyRule <- edgeRule.propertyRules
      if propertyRule.key.isNotParsed
    } yield PropertyDef(PropertyType.Edge, safeValue(propertyRule.key), dataTypeParser.get(propertyRule.value), unique = false, indexed = true)

    vertexGbIdPropertyDefs ++ vertexPropertyDefs ++ edgePropertyDefs
  }

  /**
   * Get the value from the key with extra safety and error checking
   */
  private[schema] def safeValue(key: Value): String = {
    if (key.isNotParsed) {
      StringUtils.nullSafeToString(key.value)
    }
    else {
      throw new RuntimeException("Unexpected: this method should not be called with parsed values: " + key)
    }
  }

}
