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


package org.trustedanalytics.atk.plugins.exportfromtitan

import org.trustedanalytics.atk.engine.graph.plugins.exportfromtitan.VertexSchemaAggregator
import org.trustedanalytics.atk.graphbuilder.elements.{ GBVertex, Property }
import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.scalatest.WordSpec

class VertexSchemaAggregatorTest extends WordSpec {

  val movieVertex = GBVertex(1L, Property("titanPhysicalId", 2L), Set(Property(GraphSchema.labelProperty, "movie"), Property("movieId", 23L)))
  val userVertex = GBVertex(2L, Property("titanPhysicalId", 3L), Set(Property(GraphSchema.labelProperty, "user"), Property("userId", 52L)))
  val vertexSchemaAggregator = new VertexSchemaAggregator(List("movieId", "userId"))

  val zeroValue = vertexSchemaAggregator.zeroValue

  "VertexSchemaAggregator" should {
    "convert movie GBVertices to VertexSchemas" in {
      val schema = vertexSchemaAggregator.toSchema(movieVertex)
      assert(schema.label == "movie")
      assert(schema.hasColumns(Seq(GraphSchema.vidProperty, GraphSchema.labelProperty, "movieId")))
      assert(schema.columns.size == 3, "expected _vid, _label, and movieId")
    }

    "convert user GBVertices to VertexSchemas" in {
      val schema = vertexSchemaAggregator.toSchema(userVertex)
      assert(schema.label == "user")
      assert(schema.hasColumns(Seq(GraphSchema.vidProperty, GraphSchema.labelProperty, "userId")))
      assert(schema.columns.size == 3, "expected _vid, _label, and userId")
    }

    "combining zero values should still be a zero value" in {
      assert(vertexSchemaAggregator.combOp(zeroValue, zeroValue) == zeroValue)
    }

    "aggregating one vertex more than once should not change the output" in {
      val agg1 = vertexSchemaAggregator.seqOp(zeroValue, movieVertex)
      val agg2 = vertexSchemaAggregator.seqOp(agg1, movieVertex)
      assert(agg1 == agg2)

      val agg3 = vertexSchemaAggregator.combOp(agg1, agg2)
      assert(agg1 == agg3)
    }

    "aggregating two vertex types should give two schemas" in {
      val agg1 = vertexSchemaAggregator.seqOp(zeroValue, movieVertex)
      val agg2 = vertexSchemaAggregator.seqOp(agg1, userVertex)

      assert(agg2.keySet.contains("user"))
      assert(agg2.keySet.contains("movie"))
      assert(agg2.size == 2)
    }

    "combing two vertex types multiple times should not change the aggregation" in {
      val agg1 = vertexSchemaAggregator.seqOp(zeroValue, movieVertex)
      val agg2 = vertexSchemaAggregator.seqOp(zeroValue, userVertex)

      val agg3 = vertexSchemaAggregator.combOp(agg1, agg2)

      // none of the following changes should make a difference
      val agg4 = vertexSchemaAggregator.combOp(agg3, agg2)
      val agg5 = vertexSchemaAggregator.combOp(agg1, agg4)
      val agg6 = vertexSchemaAggregator.combOp(zeroValue, agg5)
      val agg7 = vertexSchemaAggregator.combOp(agg3, agg3)

      assert(agg3 == agg4)
      assert(agg3 == agg5)
      assert(agg3 == agg6)
      assert(agg3 == agg7)
    }
  }

}
