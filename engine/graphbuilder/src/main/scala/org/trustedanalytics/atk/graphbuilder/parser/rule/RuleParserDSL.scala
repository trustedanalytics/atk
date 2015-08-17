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

package org.trustedanalytics.atk.graphbuilder.parser.rule

/**
 * Methods and implicit conversions that help the creation of ParserRules to be simpler and use a more DSL-like syntax
 */
object RuleParserDSL {

  /**
   * Convert Any to a ConstantValue
   */
  implicit def anyToConstantValue(value: Any) = {
    new ConstantValue(value)
  }

  /**
   * Convert a single EdgeRule to a List containing one EdgeRule
   */
  implicit def edgeRuleToEdgeRuleList(edgeRule: EdgeRule) = {
    List(edgeRule)
  }

  /**
   * Convert a single VertexRule to a List containing one VertexRule
   */
  implicit def vertexRuleToVertexRuleList(vertexRule: VertexRule) = {
    List(vertexRule)
  }

  /**
   * Convert a single PropertyRule to a List containing one PropertyRule
   */
  implicit def propertyRuleToPropertyRuleList(propertyRule: PropertyRule) = {
    List(propertyRule)
  }

  /**
   * If you want a CompoundValue to start with a StaticValue you should use label().
   *
   * E.g. label("myConstantLabel") + column("columnName")
   */
  def constant(value: Any): ConstantValue = {
    new ConstantValue(value)
  }

  /**
   * Defines a ParsedValue by providing a column name for the src of the Value
   * @param columnName from input row
   */
  def column(columnName: String): ParsedValue = {
    new ParsedValue(columnName)
  }

  /**
   * Define a PropertyRule where the source for the columnName will also be used as the key name in the target
   * @param columnName from input row
   */
  def property(columnName: String): PropertyRule = {
    new PropertyRule(new ConstantValue(columnName), new ParsedValue(columnName))
  }

  /**
   * Create a PropertyRule with a key of "gbId" and a value parsed from the columnName supplied
   * @param columnName from input row
   */
  // TODO: keep? Does anyone ever want their ids called "gbId"
  def gbId(columnName: String): PropertyRule = {
    new PropertyRule(new ConstantValue("gbId"), new ParsedValue(columnName))
  }
}
