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

import org.trustedanalytics.atk.graphbuilder.parser.rule.Value
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ Value, ParsedValue, CompoundValue, ConstantValue }
import org.mockito.Mockito._
import org.trustedanalytics.atk.graphbuilder.parser.rule.ParsedValue
import org.trustedanalytics.atk.graphbuilder.parser.rule.CompoundValue
import org.trustedanalytics.atk.graphbuilder.parser.rule.ConstantValue
import org.scalatest.mock.MockitoSugar

class ValueTest extends WordSpec with Matchers with MockitoSugar {

  "Value" should {

    "support constant hard-coded values" in {
      val hardcoded = "hardcoded"
      val value = new ConstantValue(hardcoded)
      value.value shouldBe hardcoded
    }
  }

  "ConstantValue" should {
    "never be considered parsed (String)" in {
      new ConstantValue("any").isParsed shouldBe false
    }

    "never be considered parsed (Int)" in {
      new ConstantValue(0).isParsed shouldBe false
    }

    "never be considered parsed (empty list)" in {
      new ConstantValue(Nil).isParsed shouldBe false
    }
  }

  "ParsedValue" should {
    "always be considered parsed" in {
      new ParsedValue("any").isParsed shouldBe true
    }

    "throw an exception for empty column names" in {
      an[IllegalArgumentException] should be thrownBy new ParsedValue("")
    }

    "throw an exception for null column names" in {
      an[IllegalArgumentException] should be thrownBy new ParsedValue(null)
    }

    "throw an exception if value() is called without a row" in {
      an[RuntimeException] should be thrownBy new ParsedValue("any").value
    }

  }

  "CompoundValue" should {
    "never be considered parsed when fully composed of constant values" in {
      new CompoundValue(new ConstantValue("any"), new ConstantValue("any")).isParsed shouldBe false
    }

    "always be considered parsed when first value is a parsed value" in {
      new CompoundValue(new ParsedValue("any"), new ConstantValue("any")).isParsed shouldBe true
    }

    "always be considered parsed when second value is a parsed value" in {
      new CompoundValue(new ConstantValue("any"), new ParsedValue("any")).isParsed shouldBe true
    }

    "always be considered parsed when fully composed of parsed values" in {
      new CompoundValue(new ParsedValue("any"), new ParsedValue("any")).isParsed shouldBe true
    }

    "delegate 'in' to underlying values" in {
      // setup mocks
      val row = mock[InputRow]
      val valueInTrue = mock[Value]
      val valueInFalse = mock[Value]
      when(valueInTrue.in(row)).thenReturn(true)
      when(valueInFalse.in(row)).thenReturn(false)

      // invoke method under test
      new CompoundValue(valueInTrue, valueInTrue).in(row) shouldBe true
      new CompoundValue(valueInTrue, valueInFalse).in(row) shouldBe false
      new CompoundValue(valueInFalse, valueInTrue).in(row) shouldBe false
      new CompoundValue(valueInFalse, valueInFalse).in(row) shouldBe false
    }

    "concatenate underlying values" in {
      // setup mocks
      val value1 = mock[Value]
      val value2 = mock[Value]
      when(value1.value).thenReturn("111", None)
      when(value2.value).thenReturn("222", None)

      // invoke method under test
      new CompoundValue(value1, value2).value shouldBe "111222"
    }

    "concatenate underlying values not dying on null constants" in {
      // invoke method under test
      new CompoundValue(new ConstantValue(null), new ConstantValue(null)).value shouldBe "null"
    }
  }

}
