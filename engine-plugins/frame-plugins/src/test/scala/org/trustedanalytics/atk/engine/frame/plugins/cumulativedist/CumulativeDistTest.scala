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

package org.trustedanalytics.atk.engine.frame.plugins.cumulativedist

import org.apache.spark.SparkException
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class CumulativeDistTest extends TestingSparkContextFlatSpec with Matchers {

  val inputList = List(
    Row(0, 0, "a", 0),
    Row(1, 1, "b", 0),
    Row(2, 2, "c", 0),
    Row(3, 0, "a", 0),
    Row(4, 1, "b", 0),
    Row(5, 2, "c", 0))

  val inputSchema = FrameSchema(List(
    Column("col_0", DataTypes.int32),
    Column("col_1", DataTypes.int32),
    Column("col_2", DataTypes.string),
    Column("col_3", DataTypes.int32)
  ))

  "cumulative sum" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativeSum(frame, "col_1")
    val result = resultRdd.take(6)

    result(0) shouldBe Row(0, 0, "a", 0, 0)
    result(1) shouldBe Row(1, 1, "b", 0, 1)
    result(2) shouldBe Row(2, 2, "c", 0, 3)
    result(3) shouldBe Row(3, 0, "a", 0, 3)
    result(4) shouldBe Row(4, 1, "b", 0, 4)
    result(5) shouldBe Row(5, 2, "c", 0, 6)
  }

  "cumulative sum" should "compute correct distribution with different partitioning" in {
    for {
      i <- 1 to 10
    } {
      val rdd = sparkContext.parallelize(inputList).sortBy(row => row.getInt(0), ascending = true, numPartitions = i)
      val frame = new FrameRdd(inputSchema, rdd)
      val resultRdd = CumulativeDistFunctions.cumulativeSum(frame, "col_1")
      val result = resultRdd.take(6)

      result(0) shouldBe Row(0, 0, "a", 0, 0)
      result(1) shouldBe Row(1, 1, "b", 0, 1)
      result(2) shouldBe Row(2, 2, "c", 0, 3)
      result(3) shouldBe Row(3, 0, "a", 0, 3)
      result(4) shouldBe Row(4, 1, "b", 0, 4)
      result(5) shouldBe Row(5, 2, "c", 0, 6)
    }
  }

  "cumulative sum" should "compute correct distribution with over partitioning" in {
    val rdd = sparkContext.parallelize(inputList).sortBy(row => row.getInt(0), ascending = true, numPartitions = 1000)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativeSum(frame, "col_1")
    val result = resultRdd.take(6)

    result(0) shouldBe Row(0, 0, "a", 0, 0)
    result(1) shouldBe Row(1, 1, "b", 0, 1)
    result(2) shouldBe Row(2, 2, "c", 0, 3)
    result(3) shouldBe Row(3, 0, "a", 0, 3)
    result(4) shouldBe Row(4, 1, "b", 0, 4)
    result(5) shouldBe Row(5, 2, "c", 0, 6)
  }

  "cumulative sum" should "throw error for non-numeric columns" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    a[SparkException] shouldBe thrownBy(CumulativeDistFunctions.cumulativeSum(frame, "col_2"))
  }

  "cumulative sum" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativeSum(frame, "col_3")
    val result = resultRdd.take(6)

    result(0) shouldBe Row(0, 0, "a", 0, 0)
    result(1) shouldBe Row(1, 1, "b", 0, 0)
    result(2) shouldBe Row(2, 2, "c", 0, 0)
    result(3) shouldBe Row(3, 0, "a", 0, 0)
    result(4) shouldBe Row(4, 1, "b", 0, 0)
    result(5) shouldBe Row(5, 2, "c", 0, 0)
  }

  "cumulative count" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativeCount(frame, "col_1", "1")
    val result = resultRdd.take(6)

    result(0) shouldBe Row(0, 0, "a", 0, 0)
    result(1) shouldBe Row(1, 1, "b", 0, 1)
    result(2) shouldBe Row(2, 2, "c", 0, 1)
    result(3) shouldBe Row(3, 0, "a", 0, 1)
    result(4) shouldBe Row(4, 1, "b", 0, 2)
    result(5) shouldBe Row(5, 2, "c", 0, 2)
  }

  "cumulative count" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativeCount(frame, "col_3", "0")
    val result = resultRdd.take(6)

    result(0) shouldBe Row(0, 0, "a", 0, 1)
    result(1) shouldBe Row(1, 1, "b", 0, 2)
    result(2) shouldBe Row(2, 2, "c", 0, 3)
    result(3) shouldBe Row(3, 0, "a", 0, 4)
    result(4) shouldBe Row(4, 1, "b", 0, 5)
    result(5) shouldBe Row(5, 2, "c", 0, 6)
  }

  "cumulative count" should "compute correct distribution for column of strings" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativeCount(frame, "col_2", "b")
    val result = resultRdd.take(6)

    result(0) shouldBe Row(0, 0, "a", 0, 0)
    result(1) shouldBe Row(1, 1, "b", 0, 1)
    result(2) shouldBe Row(2, 2, "c", 0, 1)
    result(3) shouldBe Row(3, 0, "a", 0, 1)
    result(4) shouldBe Row(4, 1, "b", 0, 2)
    result(5) shouldBe Row(5, 2, "c", 0, 2)
  }

  "cumulative percent sum" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativePercentSum(frame, "col_1")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result(0)(4).toString) shouldEqual 0
    var diff = (java.lang.Double.parseDouble(result(1)(4).toString) - 0.16666666).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result(2)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(3)(4).toString) shouldEqual 0.5
    diff = (java.lang.Double.parseDouble(result(4)(4).toString) - 0.66666666).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result(5)(4).toString) shouldEqual 1
  }

  "cumulative percent sum" should "throw error for non-numeric columns" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    a[SparkException] shouldBe thrownBy(CumulativeDistFunctions.cumulativePercentSum(frame, "col_2"))
  }

  "cumulative percent sum" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativePercentSum(frame, "col_3")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result(0)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(1)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(2)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(3)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(4)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(5)(4).toString) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativePercentCount(frame, "col_1", "1")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result(0)(4).toString) shouldEqual 0
    java.lang.Double.parseDouble(result(1)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(2)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(3)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(4)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(5)(4).toString) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution for column of all zero" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativePercentCount(frame, "col_3", "0")
    val result = resultRdd.take(6)

    var diff = (java.lang.Double.parseDouble(result(0)(4).toString) - 0.16666666).abs
    diff should be <= 0.00000001
    diff = (java.lang.Double.parseDouble(result(1)(4).toString) - 0.33333333).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result(2)(4).toString) shouldEqual 0.5
    diff = (java.lang.Double.parseDouble(result(3)(4).toString) - 0.66666666).abs
    diff should be <= 0.00000001
    diff = (java.lang.Double.parseDouble(result(4)(4).toString) - 0.83333333).abs
    diff should be <= 0.00000001
    java.lang.Double.parseDouble(result(5)(4).toString) shouldEqual 1
  }

  "cumulative percent count" should "compute correct distribution for column of strings" in {
    val rdd = sparkContext.parallelize(inputList)
    val frame = new FrameRdd(inputSchema, rdd)
    val resultRdd = CumulativeDistFunctions.cumulativePercentCount(frame, "col_2", "b")
    val result = resultRdd.take(6)

    java.lang.Double.parseDouble(result(0)(4).toString) shouldEqual 0
    java.lang.Double.parseDouble(result(1)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(2)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(3)(4).toString) shouldEqual 0.5
    java.lang.Double.parseDouble(result(4)(4).toString) shouldEqual 1
    java.lang.Double.parseDouble(result(5)(4).toString) shouldEqual 1
  }

  val doubles = List(1.0, 7.0, 30.0, 50.0, 100.0)

  "paritionSums" should "compute correct sum for one partition" in {
    val rdd = sparkContext.parallelize(doubles).sortBy(identity, ascending = true, numPartitions = 1)
    val result = CumulativeDistFunctions.partitionSums(rdd)
    assert(result === Array(0.0, 188.0))
  }

  "paritionSums" should "compute correct sums for two partitions" in {
    val rdd = sparkContext.parallelize(doubles).sortBy(identity, ascending = true, numPartitions = 2)
    val result = CumulativeDistFunctions.partitionSums(rdd)
    assert(result === Array(0.0, 38.0, 150.0))
  }

  "paritionSums" should "compute correct sums for three partitions" in {
    val rdd = sparkContext.parallelize(doubles).sortBy(identity, ascending = true, numPartitions = 3)
    val result = CumulativeDistFunctions.partitionSums(rdd)
    assert(result === Array(0.0, 8.0, 80.0, 100.0))
  }

  "paritionSums" should "compute correct sums for seven partitions" in {
    val rdd = sparkContext.parallelize(doubles).sortBy(identity, ascending = true, numPartitions = 7)
    val result = CumulativeDistFunctions.partitionSums(rdd)
    assert(result === Array(0.0, 1.0, 7.0, 30.0, 50.0, 100.0, 0.0))
  }

}
