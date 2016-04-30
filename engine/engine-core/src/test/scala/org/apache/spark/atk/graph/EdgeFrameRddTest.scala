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
package org.apache.spark.atk.graph

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.{ Matchers, WordSpec }
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

import scala.collection.mutable.ArrayBuffer

/**
 * Scala unit test for mapPartitionEdges() method in EdgeFrameRdd
 */
class EdgeFrameRddTest extends WordSpec with Matchers with TestingSparkContextWordSpec {

  val columns = List(Column(GraphSchema.edgeProperty, DataTypes.int64), Column(GraphSchema.srcVidProperty, DataTypes.int64), Column(GraphSchema.destVidProperty, DataTypes.int64), Column(GraphSchema.labelProperty, DataTypes.string), Column("distance", DataTypes.int32))
  val schema = new EdgeSchema(columns, "label", "srclabel", "destlabel")
  val rows: List[Row] = List(
    new GenericRow(Array(1L, 1L, 2L, "distance1", 100)),
    new GenericRow(Array(2L, 1L, 3L, "distance2", 200)),
    new GenericRow(Array(3L, 3L, 4L, "distance3", 400)),
    new GenericRow(Array(4L, 2L, 3L, "distance4", 500)))

  "EdgeFrameRdd" should {
    "execute user-defined edge functions in mapPartitionEdges()" in {
      val rowRdd = sparkContext.parallelize(rows)
      val edgeFrameRdd = new EdgeFrameRdd(schema, rowRdd)
      val nameRdd = edgeFrameRdd.mapPartitionEdges(iter => {
        val nameBuffer = new ArrayBuffer[String]()

        while (iter.hasNext) {
          val edgeWrapper = iter.next() //Atk edge wrapper
          val nameProperty = edgeWrapper.stringValue("distance")
          nameBuffer += nameProperty
        }
        nameBuffer.toIterator
      })
      //collect the nameRdd into an array
      val nameList = nameRdd.collect()
      nameList should contain theSameElementsAs (Array("100", "200", "400", "500"))

    }
  }

}