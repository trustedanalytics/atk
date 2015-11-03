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
import org.apache.spark.atk.graph.EdgeWrapper
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ FlatSpec, Matchers }

class EdgeWrapperTest extends FlatSpec with Matchers {

  val columns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))

  val schema = new EdgeSchema(columns, "label", "srclabel", "destlabel")

  "EdgeWrapper" should "allow access to underlying edge data" in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    wrapper.eid shouldBe 1L
    wrapper.label shouldBe "distance"

    wrapper.srcVertexId shouldBe 2L
    wrapper.destVertexId shouldBe 3L
    wrapper.intValue("distance") shouldBe 500
  }

  "get property value" should "raise exception when the property is not valid" in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)

    //make sure it throw exception when accessing invalid property
    intercept[IllegalArgumentException] {
      wrapper.stringValue("random")
    }
  }

  "hasProperty" should "return false if property does not exist " in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    wrapper.hasProperty(GraphSchema.srcVidProperty) shouldBe true
    wrapper.hasProperty("distance") shouldBe true
    wrapper.hasProperty("random_column") shouldBe false
  }

  "EdgeWrapper" should "allow modifying edge data" ignore {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    wrapper.setValue("distance", 350)
    wrapper.intValue("distance") shouldBe 350
  }

  "EdgeWrapper" should "create a GBEdge Type" in {
    val wrapper = new EdgeWrapper(schema)
    val row = new GenericRow(Array(1L, 2L, 3L, "distance", 500))
    wrapper(row)
    val gbEdge = wrapper.toGbEdge
    gbEdge.label should be("label")
    gbEdge.getProperty("distance").get.value should be(500)
    gbEdge.getProperty(GraphSchema.edgeProperty).get.value should be(1)
    gbEdge.tailVertexGbId.key should be(GraphSchema.vidProperty)
    gbEdge.headVertexGbId.key should be(GraphSchema.vidProperty)
    gbEdge.tailVertexGbId.value should be(2L)
    gbEdge.headVertexGbId.value should be(3L)
  }
}
