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

package org.trustedanalytics.atk.graphbuilder.parser

import org.trustedanalytics.atk.graphbuilder.parser.rule.RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.rule._
import org.scalatest.{ Matchers, WordSpec }

class CombinedParserTest extends WordSpec with Matchers {

  "CompinedParser" should {

    "in the numbers example, parse 15 graph elements" in {

      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y"))

      val parser = {
        val inputSchema = new InputSchema(List("cf:number", "cf:factor", "binary", "isPrime", "reverse", "isPalindrome"), null)
        val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
        val edgeRules = List(EdgeRule(property("cf:number"), property("reverse"), constant("reverseOf")))
        new CombinedParser(inputSchema, new VertexRuleParser(inputSchema, vertexRules), new EdgeRuleParser(inputSchema, edgeRules))
      }

      inputRows.flatMap(row => parser.parse(row)).size shouldBe 15
    }

  }
}
