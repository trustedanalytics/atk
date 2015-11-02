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


package org.trustedanalytics.atk.spray.json

import org.scalatest.{ FlatSpec, Matchers }

import spray.json._
import AtkDefaultJsonProtocol._

class AtkDefaultJsonProtocolTest extends FlatSpec with Matchers {

  /** For testing case class toJson and back */
  case class Foo(lower: String, mixedCase: String, under_score: String)

  case class Bar(fooA: Foo, fooB: Foo)

  implicit val fooFormat = jsonFormat3(Foo)
  implicit val barFormat = jsonFormat2(Bar)

  "CustomJsonProtocol" should "support converting a case class using camelCase to JSON with underscores" in {
    val foo = Foo("AAA", "BBB", "CCC")
    val json = foo.toJson.compactPrint
    assert(json.contains("mixed_case"), "JSON did not contain underscores: " + json)
  }

  it should "support reading JSON with underscores and converting to camelCase names in a case class" in {
    val json = JsonParser("{\"lower\":\"AAA\",\"mixed_case\":\"BBB\",\"under_score\":\"CCC\"}")
    val foo = json.convertTo[Foo]
    assert(foo.lower == "AAA")
    assert(foo.mixedCase == "BBB")
    assert(foo.under_score == "CCC")
  }

  it should "support reading JSON with randomly ordered properties" in {
    val json = JsonParser("{\"mixed_case\":\"BBB\",\"under_score\":\"CCC\",\"lower\":\"AAA\"}")
    val foo = json.convertTo[Foo]
    assert(foo.lower == "AAA")
    assert(foo.mixedCase == "BBB")
    assert(foo.under_score == "CCC")
  }

  it should "support converting nested case classes to JSON" in {
    val fooA = Foo("AAA", "BBB", "CCC")
    val fooB = Foo("111", "222", "333")
    val bar = Bar(fooA, fooB)
    val json = bar.toJson.prettyPrint
    assert(json == """{
                     |  "foo_a": {
                     |    "lower": "AAA",
                     |    "mixed_case": "BBB",
                     |    "under_score": "CCC"
                     |  },
                     |  "foo_b": {
                     |    "lower": "111",
                     |    "mixed_case": "222",
                     |    "under_score": "333"
                     |  }
                     |}""".stripMargin)
  }

  it should "support converting JSON to nested case classes" in {
    val json = JsonParser("""{
                             |  "foo_a": {
                             |    "lower": "AAA",
                             |    "mixed_case": "BBB",
                             |    "under_score": "CCC"
                             |  },
                             |  "foo_b": {
                             |    "lower": "111",
                             |    "mixed_case": "222",
                             |    "under_score": "333"
                             |  }
                             |}""".stripMargin)

    val fooA = Foo("AAA", "BBB", "CCC")
    val fooB = Foo("111", "222", "333")
    val bar = Bar(fooA, fooB)
    assert(json.convertTo[Bar] == bar)
  }

}
