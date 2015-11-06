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

import org.scalatest.{ Matchers, WordSpec }

class InputSchemaTest extends WordSpec with Matchers {

  "ColumnDef" should {

    "require a non-empty column name" in {
      an[IllegalArgumentException] should be thrownBy new ColumnDef("", null, null)
    }

    "require a non-null column name" in {
      an[IllegalArgumentException] should be thrownBy new ColumnDef(null, null, null)
    }

    "require an index greater than or equal to zero" in {
      an[IllegalArgumentException] should be thrownBy new ColumnDef("any", null, -1)
    }
  }

  "InputSchema" should {

    val columnDefs = List(new ColumnDef("col1", classOf[String], null), new ColumnDef("col2", classOf[Int], null))
    val inputSchema = new InputSchema(columnDefs)

    "keep track of the number of columns" in {
      inputSchema.size shouldBe 2
    }

    "report dataType correctly for String" in {
      inputSchema.columnType("col1") shouldBe classOf[String]
    }

    "report dataType correctly for Int" in {
      inputSchema.columnType("col2") shouldBe classOf[Int]
    }

    "calculate and report the index of 1st column correctly" in {
      inputSchema.columnIndex("col1") shouldBe 0
    }

    "calculate and report the index of 2nd column correctly" in {
      inputSchema.columnIndex("col2") shouldBe 1
    }

    "accept indexes provided as part of the column definition" in {
      val columnDefs = List(new ColumnDef("col1", classOf[String], 1), new ColumnDef("col2", classOf[Int], 0))
      val inputSchema = new InputSchema(columnDefs)
      inputSchema.columnIndex("col1") shouldBe 1
      inputSchema.columnIndex("col2") shouldBe 0
    }
  }
}
