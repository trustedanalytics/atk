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


package org.trustedanalytics.atk.engine.frame.plugins.bincolumn

import org.apache.spark.{ sql, SparkException }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class BinColumnITest extends TestingSparkContextFlatSpec with Matchers {

  "binEqualWidth" should "append new column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualWidth(1, 2, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 5
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 0)
    result.apply(2) shouldBe Row("C", 3, 1)
    result.apply(3) shouldBe Row("D", 4, 1)
    result.apply(4) shouldBe Row("E", 5, 1)
  }

  "binEqualWidth" should "create the correct number of bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualWidth(1, 2, rdd).rdd

    // Validate
    binnedRdd.map(row => row(2)).distinct.count() shouldEqual 2
  }

  "binEqualWidth" should "create equal width bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.5),
      Array[Any]("C", 2),
      Array[Any]("D", 3),
      Array[Any]("E", 4),
      Array[Any]("F", 4.5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualWidth(1, 4, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 6
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 1.5, 0)
    result.apply(2) shouldBe Row("C", 2, 1)
    result.apply(3) shouldBe Row("D", 3, 2)
    result.apply(4) shouldBe Row("E", 4, 3)
    result.apply(5) shouldBe Row("F", 4.5, 3)
  }

  "binEqualWidth" should "throw error if less than one bin requested" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.5),
      Array[Any]("C", 2),
      Array[Any]("D", 3),
      Array[Any]("E", 4),
      Array[Any]("F", 4.5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList).map(row => new GenericRow(row))

    // Get binned results
    an[IllegalArgumentException] shouldBe thrownBy(DiscretizationFunctions.binEqualWidth(1, 0, rdd))
  }

  "binEqualWidth" should "throw error if attempting to bin non-numeric column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.5),
      Array[Any]("C", 2),
      Array[Any]("D", 3),
      Array[Any]("E", 4),
      Array[Any]("F", 4.5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList).map(row => new GenericRow(row))

    // Get binned results
    a[SparkException] shouldBe thrownBy(DiscretizationFunctions.binEqualWidth(0, 4, rdd))
  }

  "binEqualWidth" should "put each element in separate bin if num_bins is greater than length of column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualWidth(1, 20, rdd).rdd // note this creates bins of width 0.55 for this dataset
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 10
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 2)
    result.apply(2) shouldBe Row("C", 3, 4)
    result.apply(3) shouldBe Row("D", 4, 6)
    result.apply(4) shouldBe Row("E", 5, 8)
    result.apply(5) shouldBe Row("F", 6, 11)
    result.apply(6) shouldBe Row("G", 7, 13)
    result.apply(7) shouldBe Row("H", 8, 15)
    result.apply(8) shouldBe Row("I", 9, 17)
    result.apply(9) shouldBe Row("J", 10, 19)
  }

  "binEqualDepth" should "append new column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualDepth(1, 2, None, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 5
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 0)
    result.apply(2) shouldBe Row("C", 3, 1)
    result.apply(3) shouldBe Row("D", 4, 1)
    result.apply(4) shouldBe Row("E", 5, 1)
  }

  "binEqualDepth" should "create the correct number of bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualDepth(1, 2, None, rdd).rdd

    // Validate
    binnedRdd.map(row => row(2)).distinct.count() shouldEqual 2
  }

  "binEqualDepth" should "bin identical values in same bin, even if it means creating fewer than requested bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1),
      Array[Any]("C", 1),
      Array[Any]("D", 1),
      Array[Any]("E", 5))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualDepth(1, 3, None, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 5
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 1, 0)
    result.apply(2) shouldBe Row("C", 1, 0)
    result.apply(3) shouldBe Row("D", 1, 0)
    result.apply(4) shouldBe Row("E", 5, 1)
  }

  "binEqualDepth" should "create equal depth bins" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 1.2),
      Array[Any]("C", 1.5),
      Array[Any]("D", 1.6),
      Array[Any]("E", 3),
      Array[Any]("F", 6))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualDepth(1, 3, None, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 6
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 1.2, 0)
    result.apply(2) shouldBe Row("C", 1.5, 1)
    result.apply(3) shouldBe Row("D", 1.6, 1)
    result.apply(4) shouldBe Row("E", 3, 2)
    result.apply(5) shouldBe Row("F", 6, 2)
  }

  "binEqualDepth" should "create equal depth bins - another test" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualDepth(1, 2, None, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 10
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 0)
    result.apply(2) shouldBe Row("C", 3, 0)
    result.apply(3) shouldBe Row("D", 4, 0)
    result.apply(4) shouldBe Row("E", 5, 0)
    result.apply(5) shouldBe Row("F", 6, 1)
    result.apply(6) shouldBe Row("G", 7, 1)
    result.apply(7) shouldBe Row("H", 8, 1)
    result.apply(8) shouldBe Row("I", 9, 1)
    result.apply(9) shouldBe Row("J", 10, 1)
  }

  "binEqualDepth" should "put each value in separate bin if num_bins is greater than length of column" in {
    // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binEqualDepth(1, 20, None, rdd).rdd
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 10
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 1)
    result.apply(2) shouldBe Row("C", 3, 2)
    result.apply(3) shouldBe Row("D", 4, 3)
    result.apply(4) shouldBe Row("E", 5, 4)
    result.apply(5) shouldBe Row("F", 6, 5)
    result.apply(6) shouldBe Row("G", 7, 6)
    result.apply(7) shouldBe Row("H", 8, 7)
    result.apply(8) shouldBe Row("I", 9, 8)
    result.apply(9) shouldBe Row("J", 10, 9)
  }

  "binColumn" should "place values outside of cutoffs into first of last bin when strictBinning is false" in { // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binColumns(1, List(2, 4, 6, 9), lowerInclusive = true, strictBinning = false, rdd)
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 10
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 0)
    result.apply(2) shouldBe Row("C", 3, 0)
    result.apply(3) shouldBe Row("D", 4, 1)
    result.apply(4) shouldBe Row("E", 5, 1)
    result.apply(5) shouldBe Row("F", 6, 2)
    result.apply(6) shouldBe Row("G", 7, 2)
    result.apply(7) shouldBe Row("H", 8, 2)
    result.apply(8) shouldBe Row("I", 9, 2)
    result.apply(9) shouldBe Row("J", 10, 2)
  }

  "binColumn" should "place values outside of cutoffs into bin -1 when strictBinning is true" in { // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binColumns(1, List(2, 4, 6, 9), lowerInclusive = true, strictBinning = true, rdd)
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 10
    result.apply(0) shouldBe Row("A", 1, -1)
    result.apply(1) shouldBe Row("B", 2, 0)
    result.apply(2) shouldBe Row("C", 3, 0)
    result.apply(3) shouldBe Row("D", 4, 1)
    result.apply(4) shouldBe Row("E", 5, 1)
    result.apply(5) shouldBe Row("F", 6, 2)
    result.apply(6) shouldBe Row("G", 7, 2)
    result.apply(7) shouldBe Row("H", 8, 2)
    result.apply(8) shouldBe Row("I", 9, 2)
    result.apply(9) shouldBe Row("J", 10, -1)
  }

  "binColumn" should "be upper inclusive when lowerInclusive is false" in { // Input data
    val inputList = List(
      Array[Any]("A", 1),
      Array[Any]("B", 2),
      Array[Any]("C", 3),
      Array[Any]("D", 4),
      Array[Any]("E", 5),
      Array[Any]("F", 6),
      Array[Any]("G", 7),
      Array[Any]("H", 8),
      Array[Any]("I", 9),
      Array[Any]("J", 10))
    val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))

    // Get binned results
    val binnedRdd = DiscretizationFunctions.binColumns(1, List(2, 4, 6, 9), lowerInclusive = false, strictBinning = false, rdd)
    val result = binnedRdd.collect()

    // Validate
    result.length shouldBe 10
    result.apply(0) shouldBe Row("A", 1, 0)
    result.apply(1) shouldBe Row("B", 2, 0)
    result.apply(2) shouldBe Row("C", 3, 0)
    result.apply(3) shouldBe Row("D", 4, 0)
    result.apply(4) shouldBe Row("E", 5, 1)
    result.apply(5) shouldBe Row("F", 6, 1)
    result.apply(6) shouldBe Row("G", 7, 2)
    result.apply(7) shouldBe Row("H", 8, 2)
    result.apply(8) shouldBe Row("I", 9, 2)
    result.apply(9) shouldBe Row("J", 10, 2)
  }

}
