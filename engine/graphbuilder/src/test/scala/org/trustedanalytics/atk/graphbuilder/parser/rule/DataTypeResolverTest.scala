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

import org.trustedanalytics.atk.graphbuilder.parser.{ InputRow, ColumnDef, InputSchema }
import java.util.Date
import org.mockito.Mockito._
import org.scalatest.{ Matchers, WordSpec }
import org.scalatest.mock.MockitoSugar

class DataTypeResolverTest extends WordSpec with Matchers with MockitoSugar {

  val columnDef1 = new ColumnDef("columnName1", classOf[Date], 0)
  val columnDef2 = new ColumnDef("columnName2", null, 0)
  val inputSchema = new InputSchema(List(columnDef1, columnDef2))
  val dataTypeResolver = new DataTypeResolver(inputSchema)

  "DataTypeParser" should {

    "handle constant values" in {
      dataTypeResolver.get(new ConstantValue("someString")) shouldBe classOf[String]
      dataTypeResolver.get(new ConstantValue(new Date())) shouldBe classOf[Date]
    }

    "handle parsed values" in {
      dataTypeResolver.get(new ParsedValue("columnName1")) shouldBe classOf[Date]
    }

    "throw exception for parsed values without dataType" in {
      an[RuntimeException] should be thrownBy dataTypeResolver.get(new ParsedValue("columnName2"))
    }

    "handle compound values as Strings" in {
      val compoundValue = new CompoundValue(new ConstantValue(new Date()), new ConstantValue(1000))
      dataTypeResolver.get(compoundValue) shouldBe classOf[String]
    }

    "throw exception for other types not yet implemented" in {
      val unsupportedValue = new Value {
        override def value(row: InputRow): Any = null

        override def value: Any = null

        override def in(row: InputRow): Boolean = false

        override def isParsed: Boolean = false
      }

      an[RuntimeException] should be thrownBy dataTypeResolver.get(unsupportedValue)
    }
  }
}
