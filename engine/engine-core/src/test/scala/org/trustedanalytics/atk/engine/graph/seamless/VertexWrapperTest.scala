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

package org.trustedanalytics.atk.engine.graph.seamless

import org.trustedanalytics.atk.domain.schema._
import org.apache.spark.atk.graph.VertexWrapper
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ FlatSpec, Matchers }

class VertexWrapperTest extends FlatSpec with Matchers {

  val columns = List(Column(GraphSchema.vidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("name", DataTypes.string), Column("from", DataTypes.string), Column("to", DataTypes.string), Column("fair", DataTypes.int32))
  val schema = new VertexSchema(columns, GraphSchema.labelProperty, null)

  "VertexWrapper" should "allow accessing underlying vertex data" in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    wrapper.vid shouldBe 1L
    wrapper.label shouldBe "l1"

    wrapper.stringValue("name") shouldBe "Bob"
    wrapper.stringValue("from") shouldBe "PDX"
    wrapper.stringValue("to") shouldBe "LAX"
    wrapper.intValue("fair") shouldBe 350

  }

  "get property value" should "raise exception when the property is not valid" in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)

    //make sure it throw exception when accessing invalid property
    intercept[IllegalArgumentException] {
      wrapper.stringValue("random")
    }
  }

  "hasProperty" should "return false if property does not exist " in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    wrapper.hasProperty("name") shouldBe true
    wrapper.hasProperty("from") shouldBe true
    wrapper.hasProperty("random_column") shouldBe false
  }

  "VertexWrapper" should "allow modifying  vertex data" ignore {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    wrapper.setValue("from", "SFO")
    wrapper.stringValue("from") shouldBe "SFO"
  }

  "VertexWrapper" should "convert to a valid GBVertex" in {
    val wrapper = new VertexWrapper(schema)
    val row = new GenericRow(Array(1L, "l1", "Bob", "PDX", "LAX", 350))
    wrapper(row)
    val gbVertex = wrapper.toGbVertex

    gbVertex.gbId.key should be("_vid")
    gbVertex.gbId.value should be(1L)
    gbVertex.getProperty("name").get.value shouldBe "Bob"
    gbVertex.getProperty("from").get.value shouldBe "PDX"
    gbVertex.getProperty("to").get.value shouldBe "LAX"
    gbVertex.getProperty("fair").get.value shouldBe 350
  }
}
