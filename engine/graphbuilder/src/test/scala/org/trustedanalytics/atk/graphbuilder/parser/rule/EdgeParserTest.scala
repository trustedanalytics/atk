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

package org.trustedanalytics.atk.graphbuilder.parser.rule

import RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.{ InputSchema, InputRow }
import org.scalatest.{ Matchers, WordSpec }

class EdgeParserTest extends WordSpec with Matchers {

  // setup data
  val columnNames = List("userId", "userName", "age", "movieId", "movieTitle", "rating", "date", "emptyColumn", "noneColumn")
  val rowValues = List("0001", "Joe", 30, "0004", "The Titanic", 4, "Jan 2014", "", None)

  // setup parser dependencies
  val inputSchema = new InputSchema(columnNames, null)
  val inputRow = new InputRow(inputSchema, rowValues)

  "EdgeParser" should {

    "copy vertex gbId values into edge when EdgeRule matches" in {
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), constant("watched")))
      val edge = parser.parse(inputRow)

      edge.tailVertexGbId.key shouldBe "userId"
      edge.tailVertexGbId.value shouldBe "0001"

      edge.headVertexGbId.key shouldBe "movieId"
      edge.headVertexGbId.value shouldBe "0004"
    }

    "copy label value into edge when EdgeRule matches" in {
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), constant("watched")))
      val edge = parser.parse(inputRow)

      edge.label shouldBe "watched"
    }

    "handle dynamic labels parsed from input into the edge when EdgeRule matches" in {
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), column("rating")))
      val edge = parser.parse(inputRow)

      edge.label shouldBe "4" // Even though input was an Int, labels must always get converted to Strings
    }

    "parse properties into edge when EdgeRule matches" in {
      val propertyRules = List(constant("when") -> column("date"), property("rating"))
      val parser = new SingleEdgeRuleParser(new EdgeRule(property("userId"), property("movieId"), constant("watched"), propertyRules))
      val edge = parser.parse(inputRow)

      edge.properties.size shouldBe 2
      edge.properties.head.key shouldBe "when"
      edge.properties.head.value shouldBe "Jan 2014"

      edge.properties.last.key shouldBe "rating"
      edge.properties.last.value shouldBe 4
    }
  }

  "EdgeListParser" should {

    "parse 0 edges when 0 of 1 EdgeRules match" in {
      val parser = new EdgeRuleParser(inputSchema, EdgeRule(property("userId"), property("emptyColumn"), constant("watched")))
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 1 edge when 1 of 1 EdgeRules match" in {
      val propertyRules = List(constant("when") -> column("date"), property("rating"))
      val parser = new EdgeRuleParser(inputSchema, EdgeRule(property("userId"), property("movieId"), constant("watched")))
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 1 edge when 1 of 2 EdgeRules match" in {
      val edgeRules = List(EdgeRule(property("userId"), property("movieId"), constant("watched")),
        EdgeRule(property("userId"), property("emptyColumn"), constant("watched")))
      val parser = new EdgeRuleParser(inputSchema, edgeRules)
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 2 edges when 2 of 2 EdgeRules match" in {
      val edgeRules = List(new EdgeRule(property("userId"), property("movieId"), constant("watched")),
        new EdgeRule(property("movieId"), property("userId"), constant("watchedBy")))
      val parser = new EdgeRuleParser(inputSchema, edgeRules)
      parser.parse(inputRow).size shouldBe 2
    }
  }
}
