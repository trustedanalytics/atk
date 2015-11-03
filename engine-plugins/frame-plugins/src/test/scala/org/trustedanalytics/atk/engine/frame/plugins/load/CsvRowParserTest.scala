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


package org.trustedanalytics.atk.engine.frame.plugins.load

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.CsvRowParser

class CsvRowParserTest extends WordSpec with Matchers {

  val csvRowParser = new CsvRowParser(',', Array[DataTypes.DataType]())

  "RowParser" should {

    "parse a String" in {
      csvRowParser.splitLineIntoParts("a,b") shouldEqual Array("a", "b")
    }

    "parse an empty string" in {
      csvRowParser.splitLineIntoParts("") shouldEqual Array()
    }

    "parse a nested double quotes string" in {
      csvRowParser.splitLineIntoParts("foo and bar,bar and foo,\"foo, is bar\"") shouldEqual Array("foo and bar", "bar and foo", "foo, is bar")
    }

    "remove double quotes" in {
      csvRowParser.splitLineIntoParts("\"foo\",\"bar\"") shouldEqual Array("foo", "bar")
    }

    "not do anything special with single quotes" in {
      csvRowParser.splitLineIntoParts("'foo','bar'") shouldEqual Array("'foo'", "'bar'")
    }

    "parse a string with empty fields" in {
      csvRowParser.splitLineIntoParts("foo,bar,,,baz") shouldEqual Array("foo", "bar", "", "", "baz")
    }

    "preserve leading and trailing tab/s in a string" in {
      csvRowParser.splitLineIntoParts("\tfoo,bar,baz") shouldEqual Array("\tfoo", "bar", "baz")
    }

    "preserve leading and trailing spaces in a string" in {
      csvRowParser.splitLineIntoParts(" foo,bar ,baz") shouldEqual Array(" foo", "bar ", "baz")
    }

    "parse a tab separated string" in {
      val trow = new CsvRowParser('\t', Array[DataTypes.DataType]())
      trow.splitLineIntoParts("foo\tbar\tbaz") shouldEqual Array("foo", "bar", "baz")
    }
  }
}
