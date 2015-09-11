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


package org.trustedanalytics.atk.plugins.exporttotitan

import org.scalatest.WordSpec
import org.trustedanalytics.atk.domain.schema._

class SchemaConverterTest extends WordSpec {

  "SchemaConverter" should {

    "convert frame schemas to Titan Schema" in {

      val vertexSchema = VertexSchema(
        List(
          Column(GraphSchema.vidProperty, DataTypes.int64),
          Column(GraphSchema.labelProperty, DataTypes.string),
          Column("name", DataTypes.string)
        ),
        "vlabel")

      val edgeSchema = EdgeSchema(
        List(
          Column(GraphSchema.edgeProperty, DataTypes.int64),
          Column(GraphSchema.srcVidProperty, DataTypes.int64),
          Column(GraphSchema.destVidProperty, DataTypes.int64),
          Column(GraphSchema.labelProperty, DataTypes.string),
          Column("startDate", DataTypes.string)
        ), "elabel", "src", "dest")

      val graphSchema = SchemaConverter.convert(List(vertexSchema, edgeSchema))
      assert(graphSchema.edgeLabelDefs.size == 1)
      assert(graphSchema.edgeLabelDefs.head.label == "elabel")
      assert(graphSchema.propertyDefs.size == 8)
    }
  }

}
