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

class JsonMapsTest extends FlatSpec with Matchers {

  /** For testing case class toJson and back */
  case class Foo(lower: String, mixedCase: String, under_score: String)

  it should "convert a Map[String, Int] to a Map[Int, Int]" in {
    val m = Map[String, Int]("1" -> 100, "2" -> 200)
    val c = JsonMaps.convertMapKeyToInt[Int](m)
    assert(c.size == 2)
    assert(c(1) == 100)
    assert(c(2) == 200)
  }

  it should "convert a Option[Map[String, Int]] to a Map[Int, Int]" in {
    val m = Map[String, Int]("1" -> 100, "2" -> 200)
    val c = JsonMaps.convertMapKeyToInt[Int](Some(m), null)
    assert(c.size == 2)
    assert(c(1) == 100)
    assert(c(2) == 200)
  }

  it should "provide default when None is used" in {
    val d = Map[Int, Int](3 -> 300, 4 -> 400)
    val c = JsonMaps.convertMapKeyToInt[Int](None, d)
    assert(c.size == 2)
    assert(c(3) == 300)
    assert(c(4) == 400)
  }

}
