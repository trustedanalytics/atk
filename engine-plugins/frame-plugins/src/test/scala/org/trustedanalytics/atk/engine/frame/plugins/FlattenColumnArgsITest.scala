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

package org.trustedanalytics.atk.engine.frame.plugins

import org.apache.spark.sql.Row
import org.scalatest.{ BeforeAndAfterEach, FlatSpec, Matchers, Assertions }
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class FlattenColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByStringColumnIndex" should "create separate rows when flattening entries" in {
    val carOwnerShips = List(Row("Bob", "Mustang,Camry"), Row("Josh", "Neon,CLK"), Row("Alice", "PT Cruiser,Avalon,F-150"), Row("Tim", "Beatle"), Row("Becky", ""))
    val rdd = sparkContext.parallelize(carOwnerShips)
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(1), List(DataTypes.string), List(","))(rdd)
    val result = flattened.take(9)
    println(result.toList)
    result(0) shouldBe Row("Bob", "Mustang")
    result(1) shouldBe Row("Bob", "Camry")
    result(2) shouldBe Row("Josh", "Neon")
    result(3) shouldBe Row("Josh", "CLK")
    result(4) shouldBe Row("Alice", "PT Cruiser")
    result(5) shouldBe Row("Alice", "Avalon")
    result(6) shouldBe Row("Alice", "F-150")
    result(7) shouldBe Row("Tim", "Beatle")
    result(8) shouldBe Row("Becky", "")

  }

  it should "flatten a column in the dataset in less than 5ms, on average" in {
    val numIterations = 20
    val elapsedTime = new Array[Long](numIterations * 2)
    var count = 0
    val rows = List(Row("a,b,c", "10,18,20"), Row("d,e", "7,8"), Row("f,g,h", "17,2"), Row("i,j", "37,6,8"),
      Row("k,l", "12,2"), Row("m,n,o", "22,4"), Row("p", "5,80"), Row("q,r", "10,0"), Row("s,t,u", "3"))

    for (i <- 0 until numIterations) {
      val rdd = sparkContext.parallelize(rows)
      // Flatten column 1
      var startTime = System.nanoTime()
      val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(1), List(DataTypes.string), List(","))(rdd)
      var endTime = System.nanoTime()
      assertResult(19) {
        flattened.count()
      }
      elapsedTime(count) = endTime - startTime
      count += 1
      // Flatten column 0
      startTime = System.nanoTime()
      FlattenColumnFunctions.flattenRddByColumnIndices(List(0), List(DataTypes.string), List(","))(rdd)
      endTime = System.nanoTime()
      elapsedTime(count) = endTime - startTime
      count += 1
    }

    // Calculate total time, so that we can get the average
    var totalTime: Long = 0
    for (i <- elapsedTime.indices) {
      totalTime += elapsedTime(i)
    }

    // Check average time
    val average = totalTime / elapsedTime.length
    assert(average < 5000000, "Expected the average flatten column time to be less than 5000000 ns, but was " + average.toString)
  }

  it should "flatten multiple columns with strings in the dataset in less than 5 ms, on average" in {
    val numIterations = 20
    val elapsedTime = new Array[Long](numIterations)
    var count = 0
    val rows = List(Row("a,b,c", "10,18,20"), Row("d,e", "7,8"), Row("f,g,h", "17,2"), Row("i,j", "37,6,8"),
      Row("k,l", "12,2"), Row("m,n,o", "22,4"), Row("p", "5,80"), Row("q,r", "10,0"), Row("s,t,u", "3"))

    for (i <- 0 until numIterations) {
      val rdd = sparkContext.parallelize(rows)
      // Flatten columns 0 and 1
      val startTime = System.nanoTime()
      val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(0, 1), List(DataTypes.string, DataTypes.string), List(",", ","))(rdd)
      val endTime = System.nanoTime()
      assertResult(23) {
        flattened.count()
      }
      elapsedTime(count) = endTime - startTime
      count += 1
    }

    // Calculate total time, so that we can get the average
    var totalTime: Long = 0
    for (i <- elapsedTime.indices) {
      totalTime += elapsedTime(i)
    }

    // Check average time
    val average = totalTime / elapsedTime.length
    assert(average < 5000000, "Expected the average flatten column time (with strings) to be less than 5000000 ns, but was " + average.toString)
  }

  it should "flatten multiple columns with strings and vectors in less than 5 ms, on average" in {
    val elapsedTime = new Array[Long](20)
    var count = 0
    val rows = List(
      Row("a,b,c", Vector[Double](10, 18, 20)),
      Row("d,e", Vector[Double](7, 8, 5)),
      Row("f,g,h", Vector[Double](17, 2, 26)),
      Row("i,j", Vector[Double](37, 6, 8)))

    for (i <- 0 until 20) {
      val rdd = sparkContext.parallelize(rows)
      // Flatten columns 0 and 1
      val startTime = System.nanoTime()
      val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(0, 1), List(DataTypes.string, DataTypes.vector(3)), List(",", ""))(rdd)
      val endTime = System.nanoTime()
      assertResult(12) {
        flattened.count()
      }
      elapsedTime(count) = endTime - startTime
      count += 1
    }

    // Calculate total time, so that we can get the average
    var totalTime: Long = 0
    for (i <- elapsedTime.indices) {
      totalTime += elapsedTime(i)
    }

    /// Check average time
    val average = totalTime / elapsedTime.length
    assert(average < 5000000, "Expected the average flatten column time (string and vectors) to be less than 5000000 ns, but was " + average.toString)
  }

  it should "flatten multiple columns with empty strings when the number of items in each column do not match" in {
    val columnIndexes = List(0, 1)
    val delimiters = List(",", ",")
    val rows = List(Row("a,b,c", "10,18,20"), Row("d,e", "7,8"), Row("f,g,h", "17,2"), Row("i,j", "37,6,8"))
    val rdd = sparkContext.parallelize(rows)
    // Flatten columns and check row count
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(columnIndexes, List(DataTypes.string, DataTypes.string), delimiters)(rdd)
    assertResult(11) {
      flattened.count()
    }
    // Check values
    val result = flattened.take(11)
    result(0) shouldBe Row("a", "10")
    result(1) shouldBe Row("b", "18")
    result(2) shouldBe Row("c", "20")
    result(3) shouldBe Row("d", "7")
    result(4) shouldBe Row("e", "8")
    result(5) shouldBe Row("f", "17")
    result(6) shouldBe Row("g", "2")
    result(7) shouldBe Row("h", "")
    result(8) shouldBe Row("i", "37")
    result(9) shouldBe Row("j", "6")
    result(10) shouldBe Row("", "8")
  }
}

class FlattenVectorColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByVectorColumnIndex" should "create separate rows when flattening entries" in {
    val vectorShips = List(Row("counting", Vector[Double](1.2, 3.4, 5.6)),
      Row("pi", Vector[Double](3.14, 6.28, 9.42)),
      Row("neg", Vector[Double](-1.0, -5.5, 8)))
    val rdd = sparkContext.parallelize(vectorShips)
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(1), List(DataTypes.vector(3)))(rdd)
    val result = flattened.take(9)
    result(0) shouldBe Row("counting", 1.2)
    result(1) shouldBe Row("counting", 3.4)
    result(2) shouldBe Row("counting", 5.6)
    result(3) shouldBe Row("pi", 3.14)
    result(4) shouldBe Row("pi", 6.28)
    result(5) shouldBe Row("pi", 9.42)
    result(6) shouldBe Row("neg", -1.0)
    result(7) shouldBe Row("neg", -5.5)
    result(8) shouldBe Row("neg", 8)
  }

  it should "flatten a string column and a vector column in the same RDD" in {
    val rows = List(
      Row("a,b,c", Vector[Double](10, 18, 20)),
      Row("d,e", Vector[Double](7, 8, 0)),
      Row("f,g,h", Vector[Double](17, 2, 9)),
      Row("i,j", Vector[Double](37, 6, 8)),
      Row("k,l,m,n", Vector[Double](22, 2, 10)))
    val rdd = sparkContext.parallelize(rows)
    // Flatten columns and check row count
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(0, 1), List(DataTypes.string, DataTypes.vector(3)), List(",", ""))(rdd)
    assertResult(16) {
      flattened.count()
    }
    // Check values
    val result = flattened.take(16)
    result(0) shouldBe Row("a", 10)
    result(1) shouldBe Row("b", 18)
    result(2) shouldBe Row("c", 20)
    result(3) shouldBe Row("d", 7)
    result(4) shouldBe Row("e", 8)
    result(5) shouldBe Row("", 0)
    result(6) shouldBe Row("f", 17)
    result(7) shouldBe Row("g", 2)
    result(8) shouldBe Row("h", 9)
    result(9) shouldBe Row("i", 37)
    result(10) shouldBe Row("j", 6)
    result(11) shouldBe Row("", 8)
    result(12) shouldBe Row("k", 22)
    result(13) shouldBe Row("l", 2)
    result(14) shouldBe Row("m", 10)
    result(15) shouldBe Row("n", "")
  }
}
