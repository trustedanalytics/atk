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

package org.trustedanalytics.atk.shared

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.spray.json.{ ArraySchema, StringSchema, ObjectSchema, JsonSchema, NumberSchema }
import org.scalatest.{ Matchers, FlatSpec }

case class SchemaSample(other_frame: FrameReference, int: Int, long: Long,
                        string: String, frame: FrameReference, array: Array[String], option: Option[Int])

class JsonSchemaTest extends FlatSpec with Matchers {
  val schema = JsonSchemaExtractor.getProductSchema(manifest[SchemaSample])
  val reference = ObjectSchema(properties = Some(Map("int" -> JsonSchema.int(None, None).asInstanceOf[JsonSchema],
    "long" -> JsonSchema.long(None, None),
    "string" -> new StringSchema(),
    "array" -> ArraySchema(),
    "option" -> JsonSchema.int(None, None),
    "frame" -> StringSchema(format = Some("atk/frame"), self = Some(true)),
    "other_frame" -> StringSchema(format = Some("atk/graph"))
  )),
    required = Some(Array("int", "long", "string", "array", "frame", "other_frame")))

  "JsonSchemaExtractor" should "find all the case class vals" in {
    schema.properties.get.keys should equal(reference.properties.get.keys)
  }

  it should "support ints" in {
    schema.properties.get("int") should equal(reference.properties.get("int"))
  }
  it should "support Option[Int]s as ints" in {
    schema.properties.get("option") should equal(reference.properties.get("int"))
  }
  it should "make Options not required" in {
    schema.properties.get("option") should equal(reference.properties.get("option"))
    schema.required.get should not contain "option"
  }
  it should "make non-Option types required" in {
    schema.properties.get("string") should equal(reference.properties.get("string"))
    schema.required.get should contain("string")

  }

  it should "detect frames in first position as self members" in {
    schema.properties.get.get("other_frame").get.asInstanceOf[StringSchema].self should equal(Some(true))
  }

  it should "treat frames in positions other than first as non-self members" in {
    schema.properties.get.get("frame").get.asInstanceOf[StringSchema].self should equal(None)
  }

  it should "capture the order of the fields" in {
    schema.order.get should equal(Array("other_frame", "int", "long",
      "string", "frame", "array", "option"))
  }
}
