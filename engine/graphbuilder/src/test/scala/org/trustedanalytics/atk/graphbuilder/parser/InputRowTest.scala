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

import java.util.Date
import org.scalatest.{ Matchers, WordSpec }

class InputRowTest extends WordSpec with Matchers {

  val date = new Date()
  val inputSchema = new InputSchema(List("col1", "col2", "dateCol", "emptyStringCol", "nullColumn", "emptyList", "complexObject"), null)
  val values = List("abc", "def", date, "", null, Nil, Map("key" -> "value"))
  val inputRow = new InputRow(inputSchema, values)

  "InputRow" should {

    "handle String values in the input" in {
      inputRow.value("col1") shouldBe "abc"
    }

    "handle Date values in the input" in {
      inputRow.value("dateCol") shouldBe date
    }

    "handle null values in the input" in {
      assert(inputRow.value("nullColumn") == null)
    }

    "handle List values in the input" in {
      inputRow.value("emptyList") shouldBe Nil
    }

    "handle complex object values in the input" in {
      inputRow.value("complexObject") shouldBe Map("key" -> "value")
    }

    "consider a non-empty String to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("col1") shouldBe true
    }

    "consider a Date object to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("dateCol") shouldBe true
    }

    "consider the empty string to be an empty column value" in {
      inputRow.columnIsNotEmpty("emptyStringCol") shouldBe false
    }

    "consider null to be an empty column value" in {
      inputRow.columnIsNotEmpty("nullColumn") shouldBe false
    }

    "consider the empty list to be an empty column value" in {
      inputRow.columnIsNotEmpty("emptyList") shouldBe false
    }

    "consider complex objects to be a non-empty column value" in {
      inputRow.columnIsNotEmpty("complexObject") shouldBe true
    }

    "throw an exception for non-existant column names when getting value" in {
      an[NoSuchElementException] should be thrownBy inputRow.value("no-column-with-this-name")
    }

    "throw an exception for non-existant column names when checking for empty" in {
      an[NoSuchElementException] should be thrownBy inputRow.columnIsNotEmpty("no-column-with-this-name")
    }

    "throw an exception when the number of columns in schema greater than the number in the row" in {
      val inputSchema = new InputSchema(List("col1", "col2", "col3"), null)
      val values = List("abc", "def")
      an[IllegalArgumentException] should be thrownBy new InputRow(inputSchema, values)
    }

    "throw an exception when the number of columns in schema is less than the number in the row" in {
      val inputSchema = new InputSchema(List("col1", "col2"), null)
      val values = List("abc", "def", "ghi")
      an[IllegalArgumentException] should be thrownBy new InputRow(inputSchema, values)
    }
  }
}
