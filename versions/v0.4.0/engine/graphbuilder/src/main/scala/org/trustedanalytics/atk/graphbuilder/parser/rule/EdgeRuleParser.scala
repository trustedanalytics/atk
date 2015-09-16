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
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import scala.collection.mutable.ListBuffer

/**
 * Parse InputRow's into Edges using EdgeRules
 */
case class EdgeRuleParser(inputSchema: InputSchema, edgeRules: List[EdgeRule]) extends Parser[GBEdge](inputSchema) with Serializable {

  // each rule gets its own parser
  private val edgeParsers = edgeRules.map(rule => rule -> new SingleEdgeRuleParser(rule)).toMap

  /**
   * Parse the supplied InputRow into zero or more Edges using all applicable rules
   */
  def parse(row: InputRow): Seq[GBEdge] = {
    val edges = ListBuffer[GBEdge]()
    for {
      rule <- edgeRules
      if rule appliesTo row
    } {
      val edge = edgeParsers(rule).parse(row)
      edges += edge
      if (rule.biDirectional) {
        edges += edge.reverse()
      }
    }
    edges
  }
}

/**
 * Parse a single InputRow into an Edge
 */
private[graphbuilder] case class SingleEdgeRuleParser(rule: EdgeRule) extends Serializable {

  // each rule can have different rules for parsing tailVertexGbId's, headVertexGbId's, and properties
  private val tailGbIdParser = new SinglePropertyRuleParser(rule.tailVertexGbId)
  private val headGbIdParser = new SinglePropertyRuleParser(rule.headVertexGbId)
  private val propertyParser = new PropertyRuleParser(rule.propertyRules)

  def parse(row: InputRow): GBEdge = {
    new GBEdge(None, tailGbIdParser.parse(row), headGbIdParser.parse(row), rule.label.value(row), propertyParser.parse(row))
  }
}
