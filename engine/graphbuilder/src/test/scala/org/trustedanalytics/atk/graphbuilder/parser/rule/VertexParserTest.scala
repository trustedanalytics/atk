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
import org.trustedanalytics.atk.graphbuilder.parser.{ InputRow, InputSchema }
import org.scalatest.{ Matchers, WordSpec }

class VertexParserTest extends WordSpec with Matchers {

  "VertexParser" should {

    // setup data
    val columnNames = List("id", "name", "age", "managerId", "emptyColumn", "noneColumn")
    val rowValues = List("0001", "Joe", 30, "0004", "", None)

    // setup parser dependencies
    val inputSchema = new InputSchema(columnNames, null)
    val inputRow = new InputRow(inputSchema, rowValues)

    "copy the parsed gbId into created vertex" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("id")))
      val vertexList = parser.parse(inputRow)
      vertexList.head.gbId.value shouldBe "0001"
    }

    "copy the parsed gbId into created vertex from different sources" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("managerId")))
      val vertexList = parser.parse(inputRow)
      vertexList.head.gbId.value shouldBe "0004"
    }

    "parse 1 vertex when 1 of 1 rules match" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("id")))
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 2 vertices when 2 of 2 rules match" in {
      val parser = new VertexRuleParser(inputSchema, VertexRule(property("id")) :: VertexRule(property("managerId")))
      parser.parse(inputRow).size shouldBe 2
    }

    "parse 1 vertex when 1 of 2 rules match" in {
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id")) :: new VertexRule(property("noneColumn")))
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 0 vertices when 0 of 2 rules match" in {
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("emptyColumn")) :: new VertexRule(property("noneColumn")))
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 0 vertices when there are no rules" in {
      val parser = new VertexRuleParser(inputSchema, Nil)
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 0 properties when a matching vertex rule does NOT have a matching property rule" in {
      val propertyRules = new PropertyRule("employeeName", column("emptyColumn"))
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id"), propertyRules))
      val vertexList = parser.parse(inputRow)
      vertexList.size shouldBe 1
      vertexList.head.properties.size shouldBe 0
    }

    "parse 1 property when a matching vertex rule also has a matching property rule" in {
      val propertyRules = new PropertyRule("employeeName", column("name"))
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id"), propertyRules))
      val vertexList = parser.parse(inputRow)
      vertexList.size shouldBe 1
      vertexList.head.properties.size shouldBe 1
      vertexList.head.properties.head.value shouldBe "Joe"
    }

    "parse 2 properties when a matching vertex rule also has 2 matching property rules" in {
      val propertyRules = new PropertyRule("employeeName", column("name")) :: new PropertyRule("age", column("age"))
      val parser = new VertexRuleParser(inputSchema, new VertexRule(property("id"), propertyRules))
      val vertexList = parser.parse(inputRow)
      vertexList.size shouldBe 1
      vertexList.head.properties.size shouldBe 2
    }
  }
}
