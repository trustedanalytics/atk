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
    assertResult(9) {
      flattened.count()
    }
    val result = flattened.take(flattened.count().toInt)
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

  it should "flatten a single string column" in {
    val rows = List(Row("a,b,c", "10,18,20"), Row("d,e", "7,8"), Row("f,g,h", "17,2"))
    var rdd = sparkContext.parallelize(rows)

    // Flatten column 1
    var flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(1), List(DataTypes.string), List(","))(rdd)
    assertResult(7) {
      flattened.count()
    }

    // Verify that we have the rows expected after flattening column 1
    val expectedRows1 = List(Row("a,b,c", "10"), Row("a,b,c", "18"), Row("a,b,c", "20"),
      Row("d,e", "7"), Row("d,e", "8"), Row("f,g,h", "17"), Row("f,g,h", "2"))
    assertResult(expectedRows1) {
      flattened.take(flattened.count().toInt).toList
    }

    // Flatten column 0
    rdd = sparkContext.parallelize(flattened.take(flattened.count().toInt).toList)
    flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(0), List(DataTypes.string), List(","))(rdd)
    assertResult(19) {
      flattened.count()
    }

    // Verify that we have the rows expected after flattening column 0
    val expectedRows0 = List(Row("a", "10"), Row("b", "10"), Row("c", "10"),
      Row("a", "18"), Row("b", "18"), Row("c", "18"),
      Row("a", "20"), Row("b", "20"), Row("c", "20"),
      Row("d", "7"), Row("e", "7"),
      Row("d", "8"), Row("e", "8"),
      Row("f", "17"), Row("g", "17"), Row("h", "17"),
      Row("f", "2"), Row("g", "2"), Row("h", "2"))
    assertResult(expectedRows0) {
      flattened.take(flattened.count().toInt).toList
    }

  }

  it should "flatten multiple string columns" in {
    val rows = List(Row("a,b,c", "10,18,20"), Row("d,e", "7,8"), Row("f,g,h", "17,2"))
    val rdd = sparkContext.parallelize(rows)

    // Flatten column 0 and 1
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(0, 1), List(DataTypes.string, DataTypes.string), List(",", ","))(rdd)
    assertResult(8) {
      flattened.count()
    }

    val expectedRows = List(Row("a", "10"), Row("b", "18"), Row("c", "20"),
      Row("d", "7"), Row("e", "8"),
      Row("f", "17"), Row("g", "2"), Row("h", null))
    assertResult(expectedRows) {
      flattened.take(flattened.count().toInt).toList
    }
  }

  it should "flatten multiple columns with strings and vectors" in {
    val elapsedTime = new Array[Long](20)
    var count = 0
    val rows = List(
      Row("a,b,c", Vector[Double](10, 18, 20)),
      Row("d,e", Vector[Double](7, 8, 5)),
      Row("f,g,h", Vector[Double](17, 2, 26)))

    val rdd = sparkContext.parallelize(rows)

    // Flatten columns 0 and 1
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(0, 1), List(DataTypes.string, DataTypes.vector(3)), List(",", ""))(rdd)
    assertResult(9) {
      flattened.count()
    }

    val expectedRows = List(Row("a", 10), Row("b", 18), Row("c", 20),
      Row("d", 7), Row("e", 8), Row("", 5),
      Row("f", 17), Row("g", 2), Row("h", 26))

  }

  it should "flatten multiple columns with nulls when the number of items in each column do not match" in {
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
    val expectedRows = List(Row("a", "10"), Row("b", "18"), Row("c", "20"),
      Row("d", "7"), Row("e", "8"),
      Row("f", "17"), Row("g", "2"), Row("h", null),
      Row("i", "37"), Row("j", "6"), Row(null, "8"))
    assertResult(expectedRows) {
      flattened.take(flattened.count().toInt).toList
    }
  }
}

class FlattenVectorColumnArgsITest extends FlatSpec with Matchers with BeforeAndAfterEach with TestingSparkContextFlatSpec {
  "flattenRddByVectorColumnIndex" should "create separate rows when flattening entries" in {
    val vectorShips = List(Row("counting", Vector[Double](1.2, 3.4, 5.6)),
      Row("pi", Vector[Double](3.14, 6.28, 9.42)),
      Row("neg", Vector[Double](-1.0, -5.5, 8)))
    val rdd = sparkContext.parallelize(vectorShips)
    val flattened = FlattenColumnFunctions.flattenRddByColumnIndices(List(1), List(DataTypes.vector(3)))(rdd)
    assertResult(9) {
      flattened.count()
    }
    // Check values
    val expectedRows = List(Row("counting", 1.2), Row("counting", 3.4), Row("counting", 5.6),
      Row("pi", 3.14), Row("pi", 6.28), Row("pi", 9.42),
      Row("neg", -1.0), Row("neg", -5.5), Row("neg", 8))
    assertResult(expectedRows) {
      flattened.take(flattened.count().toInt).toList
    }
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
    val expectedRows = List(Row("a", 10), Row("b", 18), Row("c", 20),
      Row("d", 7), Row("e", 8), Row(null, 0),
      Row("f", 17), Row("g", 2), Row("h", 9),
      Row("i", 37), Row("j", 6), Row(null, 8),
      Row("k", 22), Row("l", 2), Row("m", 10), Row("n", 0))
    assertResult(expectedRows) {
      flattened.take(flattened.count().toInt).toList
    }
  }
}
