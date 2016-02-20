/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins.sortedk

import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class SortedKITest extends TestingSparkContextFlatSpec with Matchers {
  val inputRows: Array[Row] = Array(
    new GenericRow(Array[Any]("a", 1, 1d, "w")),
    new GenericRow(Array[Any]("c", 5, 1d, "5")),
    new GenericRow(Array[Any]("a", 5, 3d, "z")),
    new GenericRow(Array[Any]("b", -1, 1d, "1")),
    new GenericRow(Array[Any]("a", 2, 1d, "x")),
    new GenericRow(Array[Any]("b", 0, 1d, "2")),
    new GenericRow(Array[Any](null, null, 1d, "5"))
  )

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.string),
    Column("col_1", DataTypes.int32),
    Column("col_2", DataTypes.float64),
    Column("col_3", DataTypes.string)
  ))

  "sortedK" should "throw an IllegalArgumentException if K < 1" in {
    intercept[IllegalArgumentException] {
      val rdd = sparkContext.parallelize(inputRows)
      val frameRdd = new FrameRdd(inputSchema, rdd)
      val sortColumns = List(("col_0", true), ("col_1", true))
      SortedKFunctions.takeOrdered(frameRdd, -1, sortColumns).collect()
    }
  }

  "sortedK" should "return top-K rows ordered by column in ascending order" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val sortColumns = List(("col_0", true), ("col_1", true))
    val results = SortedKFunctions.takeOrdered(frameRdd, 4, sortColumns).collect()

    val expectedResults = List(
      new GenericRow(Array[Any](null, null, 1d, "5")),
      new GenericRow(Array[Any]("a", 1, 1d, "w")),
      new GenericRow(Array[Any]("a", 2, 1d, "x")),
      new GenericRow(Array[Any]("a", 5, 3d, "z"))
    )

    results.length should equal(4)
    results should contain theSameElementsInOrderAs expectedResults
  }

  "sortedK" should "return top-K rows ordered by column in descending order" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val sortColumns = List(("col_0", false), ("col_1", false))

    val results = SortedKFunctions.takeOrdered(frameRdd, 3, sortColumns).collect()

    val expectedResults = List(
      new GenericRow(Array[Any]("c", 5, 1d, "5")),
      new GenericRow(Array[Any]("b", 0, 1d, "2")),
      new GenericRow(Array[Any]("b", -1, 1d, "1"))
    )

    results.length should equal(3)
    results should contain theSameElementsInOrderAs expectedResults
  }

  "sortedK" should "return top-K rows ordered by column in mixed order" in {
    val rdd = sparkContext.parallelize(inputRows)
    val frameRdd = new FrameRdd(inputSchema, rdd)
    val sortColumns = List(("col_0", true), ("col_1", false))

    val results = SortedKFunctions.takeOrdered(frameRdd, 4, sortColumns).collect()

    val expectedResults = List(
      new GenericRow(Array[Any](null, null, 1d, "5")),
      new GenericRow(Array[Any]("a", 5, 3d, "z")),
      new GenericRow(Array[Any]("a", 2, 1d, "x")),
      new GenericRow(Array[Any]("a", 1, 1d, "w"))
    )

    results.length should equal(4)
    results should contain theSameElementsInOrderAs expectedResults
  }
}
