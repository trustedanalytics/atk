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
import org.trustedanalytics.atk.graphbuilder.parser.InputRow
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.scalatest.{ Matchers, WordSpec }

class PropertyParserTest extends WordSpec with Matchers {

  // setup data
  val columnNames = List("id", "name", "age", "managerId", "emptyColumn", "noneColumn")
  val rowValues = List("0001", "Joe", 30, "0004", "", None)

  // setup parser dependencies
  val inputSchema = new InputSchema(columnNames, null)
  val inputRow = new InputRow(inputSchema, rowValues)

  "PropertyParser" should {

    "copy the parsed String value into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("name", column("id")))
      val property = parser.parse(inputRow)
      property.value shouldBe "0001"
    }

    "copy the parsed Int value into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("age", column("age")))
      val property = parser.parse(inputRow)
      property.value shouldBe 30
    }

    "copy the constant name into the created property" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("myname", column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "myname"
    }

    "support parsing both the key and value from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule(column("name"), column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "Joe"
      property.value shouldBe "0001"
    }

    "support key and values defined as constants (neither parsed from input)" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("anyName", "anyValue"))
      val property = parser.parse(inputRow)
      property.key shouldBe "anyName"
      property.value shouldBe "anyValue"
    }

    "support adding labels to vertex id's parsed from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule("gbId", constant("USER.") + column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "gbId"
      property.value shouldBe "USER.0001"
    }

    "support parsing complex compound keys and values from the input row" in {
      val parser = new SinglePropertyRuleParser(new PropertyRule(constant("keyLabel.") + column("name") + "-" + column("id"), constant("mylabel-") + column("id")))
      val property = parser.parse(inputRow)
      property.key shouldBe "keyLabel.Joe-0001"
      property.value shouldBe "mylabel-0001"
    }

  }

  "PropertyListParser" should {

    "parse 1 property when 1 of 1 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: Nil)
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 2 properties when 2 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: new PropertyRule("managerId", column("managerId")) :: Nil)
      parser.parse(inputRow).size shouldBe 2
    }

    "parse 1 property when 1 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("id", column("id")) :: new PropertyRule("none", column("noneColumn")) :: Nil)
      parser.parse(inputRow).size shouldBe 1
    }

    "parse 0 properties when 0 of 2 rules match" in {
      val parser = new PropertyRuleParser(new PropertyRule("empty", column("emptyColumn")) :: new PropertyRule("none", column("noneColumn")) :: Nil)
      parser.parse(inputRow).size shouldBe 0
    }

    "parse 0 properties when there are no rules" in {
      val parser = new PropertyRuleParser(Nil)
      parser.parse(inputRow).size shouldBe 0
    }

  }
}
