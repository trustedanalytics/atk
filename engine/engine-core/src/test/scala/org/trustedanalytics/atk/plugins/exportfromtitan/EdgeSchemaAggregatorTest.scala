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

import org.trustedanalytics.atk.engine.graph.plugins.exportfromtitan.{ EdgeSchemaAggregator, EdgeHolder }
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBEdge }
import org.scalatest.WordSpec
import EdgeSchemaAggregator.zeroValue

class EdgeSchemaAggregatorTest extends WordSpec {

  val edge1 = GBEdge(Some(3L), 5L, 6L, Property("movieId", 8L), Property("userId", 9L), "rating", Set(Property("rating", 6)))
  val edgeHolder1 = EdgeHolder(edge1, "movies", "users")

  "EdgeSchemaAggregator" should {

    "be able to get schema from edges" in {
      val edgeSchema = EdgeSchemaAggregator.toSchema(edgeHolder1)

      assert(edgeSchema.label == "rating")
      assert(edgeSchema.srcVertexLabel == "movies")
      assert(edgeSchema.destVertexLabel == "users")
      assert(edgeSchema.hasColumn("rating"))
      assert(edgeSchema.columns.size == 5, "4 edge system columns plus one user defined column")
    }

    "combining zero values should still be a zero value" in {
      assert(EdgeSchemaAggregator.combOp(zeroValue, zeroValue) == zeroValue)
    }

    "aggregating one edge more than once should not change the output" in {
      val agg1 = EdgeSchemaAggregator.seqOp(zeroValue, edgeHolder1)
      val agg2 = EdgeSchemaAggregator.seqOp(agg1, edgeHolder1)
      assert(agg1 == agg2)

      val agg3 = EdgeSchemaAggregator.combOp(agg1, agg2)
      assert(agg1 == agg3)
    }
  }
}
