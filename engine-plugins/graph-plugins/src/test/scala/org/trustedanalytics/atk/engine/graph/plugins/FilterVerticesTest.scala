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


package org.trustedanalytics.atk.engine.graph.plugins

import org.trustedanalytics.atk.domain.schema._
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class FilterVerticesTest extends TestingSparkContextFlatSpec with Matchers {

  "dropDanglingEdgesFromEdgeRdd" should "return a edge rdd with dangling edge dropped" in {
    val edgeArray = Array(Array(1, 11, 21, "like", 100), Array(2, 12, 22, "like", 80), Array(3, 13, 23, "like", 90), Array(4, 14, 24, "like", 5))
    val edgeRdd = sparkContext.parallelize(edgeArray)

    val columns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))
    val schema = new EdgeSchema(columns, GraphSchema.labelProperty, "srclabel", "destlabel")
    val edgeLegacyRdd = FrameRdd.toFrameRdd(schema, edgeRdd)

    val vertexArray = Array(5, 13)

    val vertexRdd = sparkContext.parallelize(vertexArray).asInstanceOf[RDD[Any]]

    val remainingEdges = FilterVerticesFunctions.dropDanglingEdgesFromEdgeRdd(edgeLegacyRdd, 1, vertexRdd)
    val data = remainingEdges.collect().sortWith { case (row1, row2) => row1(0).asInstanceOf[Int] <= row2(0).asInstanceOf[Int] }.map(row => row.toSeq.toArray)
    data shouldBe Array(Array(1, 11, 21, "like", 100), Array(2, 12, 22, "like", 80), Array(4, 14, 24, "like", 5))
  }
}
