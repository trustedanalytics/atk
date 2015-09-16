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

import org.trustedanalytics.atk.graphbuilder.elements.GBVertex
import org.trustedanalytics.atk.graphbuilder.parser.{ InputRow, InputSchema, Parser }

/**
 * Parse an InputRow into Vertices using VertexRules
 */
case class VertexRuleParser(inputSchema: InputSchema, vertexRules: List[VertexRule]) extends Parser[GBVertex](inputSchema) with Serializable {

  // each rule gets its own parser
  private val vertexParsers = vertexRules.map(rule => rule -> new SingleVertexRuleParser(rule)).toMap

  /**
   * Parse the supplied InputRow into zero or more Vertices using all applicable rules
   */
  def parse(row: InputRow): Seq[GBVertex] = {
    for {
      rule <- vertexRules
      if rule appliesTo row
    } yield vertexParsers(rule).parse(row)
  }

}

/**
 * Parse an InputRow into a Vertex using a VertexRule
 */
private[graphbuilder] case class SingleVertexRuleParser(rule: VertexRule) extends Serializable {

  private val gbIdParser = new SinglePropertyRuleParser(rule.gbId)
  private val propertyParser = new PropertyRuleParser(rule.propertyRules)

  def parse(row: InputRow): GBVertex = {
    new GBVertex(gbIdParser.parse(row), propertyParser.parse(row))
  }
}
