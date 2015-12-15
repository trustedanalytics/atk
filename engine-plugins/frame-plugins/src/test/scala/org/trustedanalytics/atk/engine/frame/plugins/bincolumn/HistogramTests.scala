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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.frame.{ MissingIgnore, Missing }
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class HistogramTests extends TestingSparkContextWordSpec with Matchers {
  "Histogram" should {
    val plugin = new HistogramPlugin
    "compute data properly for weighted values" in {
      val data = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 3, .5),
        Array[Any]("D", 4, 1),
        Array[Any]("E", 5, .5))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, Some(2), numBins, MissingIgnore())
      hist.cutoffs should be(Array(1, 3, 5))
      hist.hist should be(Array(5.0, 2.0))
      hist.density should be(Array(5 / 7.0, 2 / 7.0))
    }

    "compute data properly for unweighted values" in {
      val data = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 3, .5),
        Array[Any]("D", 4, 1),
        Array[Any]("E", 5, .5))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, None, numBins, MissingIgnore())
      hist.cutoffs should be(Array(1, 3, 5))
      hist.hist should be(Array(2.0, 3.0))
      hist.density should be(Array(2 / 5.0, 3 / 5.0))
    }

    "observations with negative weights are ignored" in {
      val data = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 3, .5),
        Array[Any]("D", 4, -1),
        Array[Any]("E", 5, .5))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, Some(2), numBins, MissingIgnore())
      hist.cutoffs should be(Array(1, 3, 5))
      hist.hist should be(Array(5.0, 1.0))
      hist.density should be(Array(5 / 6.0, 1 / 6.0))
    }

    "can be computed for equal depth" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1),
        Array[Any]("B", 2),
        Array[Any]("C", 5),
        Array[Any]("D", 7),
        Array[Any]("E", 9))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, None, 2, MissingIgnore(), equalWidth = false)
      hist.cutoffs should be(Array(1, 5, 9))
      hist.hist should be(Array(2, 3))
      hist.density should be(Array(2 / 5.0, 3 / 5.0))
    }

    "equal depth supports duplicates" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1, 3),
        Array[Any]("A", 1, 3),
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 5, 1),
        Array[Any]("D", 7, 3),
        Array[Any]("D", 7, 3),
        Array[Any]("D", 7, 3),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5),
        Array[Any]("E", 9, 5))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, None, 2, MissingIgnore(), equalWidth = false)
      hist.cutoffs should be(Array(1.0, 7.0, 9.0))
      hist.hist should be(Array(6, 8))
      hist.density should be(Array(6 / 14.0, 8 / 14.0))
    }

    "equal depth supports weights" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 2),
        Array[Any]("C", 5, 1),
        Array[Any]("D", 7, 3),
        Array[Any]("E", 9, 5))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, Some(2), 2, MissingIgnore(), equalWidth = false)
      hist.cutoffs should be(Array(1.0, 7.0, 9.0))
      hist.hist should be(Array(6, 8))
      hist.density should be(Array(6 / 14.0, 8 / 14.0))
    }

    "zero weights do not throw exceptions" in {
      // Input data
      val inputList = List(
        Array[Any]("A", 1, 3),
        Array[Any]("B", 2, 0),
        Array[Any]("C", 5, -1),
        Array[Any]("D", 7, 0),
        Array[Any]("E", 9, 5))
      val rdd: RDD[Row] = sparkContext.parallelize(inputList, 2).map(row => new GenericRow(row))
      val hist = plugin.computeHistogram(rdd, 1, Some(2), 2, MissingIgnore(), equalWidth = false)
      hist.cutoffs should be(Array(1.0, 7.0, 9.0))
      hist.hist should be(Array(3, 5))
      hist.density should be(Array(3 / 8.0, 5 / 8.0))
    }

    "bins with 0 records are included" in {
      val data = List(
        Array[Any]("A", 1),
        Array[Any]("B", 2),
        Array[Any]("C", 3),
        Array[Any]("D", 9),
        Array[Any]("E", 10))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 3
      val hist = plugin.computeHistogram(rdd, 1, None, numBins, MissingIgnore())
      hist.cutoffs should be(Array(1, 4, 7, 10))
      hist.hist should be(Array(3.0, 0.0, 2.0))
      hist.density should be(Array(3 / 5.0, 0, 2 / 5.0))
    }

    "decimal datasets should always include the max value as the upper end" in {
      val data = List(
        Array[Any]("A", 1.000001),
        Array[Any]("B", 1.999999),
        Array[Any]("C", 3.000001),
        Array[Any]("D", 10.00001))
      val rdd: RDD[Row] = sparkContext.parallelize(data).map(row => new GenericRow(row))
      val numBins = 2
      val hist = plugin.computeHistogram(rdd, 1, None, numBins, MissingIgnore())
      hist.cutoffs(0) should be(1.000001)
      hist.cutoffs.last should be(10.00001)
      hist.hist should be(Array(3.0, 1.0))
      hist.density should be(Array(3 / 4.0, 1 / 4.0))
    }

  }
}
