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

import org.trustedanalytics.atk.graphbuilder.parser._

/**
 * Marker interface
 */
trait ParseRule {

}

/**
 * A VertexRule describes how to parse Vertices from InputRows
 * @param gbId a PropertyRule that defines a property will be used as the unique property for parsed vertices
 * @param propertyRules rules describing how to parse properties
 */
case class VertexRule(gbId: PropertyRule, propertyRules: List[PropertyRule] = Nil) extends ParseRule {

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    gbId appliesTo row
  }

  /**
   * The complete list of property rules including the special gbId rule
   */
  def fullPropertyRules: List[PropertyRule] = {
    gbId :: propertyRules
  }
}

/**
 * An EdgeRule describes how to parse Edges from InputRows
 * @param tailVertexGbId the rule describing how to parse the source Vertex unique Id
 * @param headVertexGbId the rule describing how to parse the destination Vertex unique Id
 * @param label the value of the Edge label
 * @param propertyRules rules describing how to parse properties
 * @param biDirectional true if this rule should produce edges in both directions
 */
case class EdgeRule(tailVertexGbId: PropertyRule,
                    headVertexGbId: PropertyRule,
                    label: Value,
                    propertyRules: List[PropertyRule] = Nil,
                    biDirectional: Boolean = false) extends ParseRule {

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    headVertexGbId.appliesTo(row) && tailVertexGbId.appliesTo(row) && label.in(row)
  }

}
