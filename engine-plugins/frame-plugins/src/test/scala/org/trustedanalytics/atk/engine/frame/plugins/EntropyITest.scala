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

package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Tests the entropy functions.
 *
 * The expected values for the tests were computed using the R entropy package.
 * @see http://cran.r-project.org/web/packages/entropy/index.html
 */
class EntropyITest extends TestingSparkContextFlatSpec with Matchers {
  val unweightedInput = List(
    Row(-1, "a", 0),
    Row(0, "a", 0),
    Row(0, "b", 0),
    Row(1, "b", 0),
    Row(1, "b", 0),
    Row(2, "c", 0))

  val weightedInput = List(
    Row("a", 1.0),
    Row("a", 1.0),
    Row("b", 0.8),
    Row("b", 0.3),
    Row("c", 0.2),
    Row("c", 0.1))

  val emptyList = List.empty[Row]

  val epsilon = 0.000001
  "shannonEntropy" should "compute the correct shannon entropy for unweighted data" in {
    val rowRDD = sparkContext.parallelize(unweightedInput, 2)
    val entropy1 = EntropyRddFunctions.shannonEntropy(rowRDD, 0)
    val entropy2 = EntropyRddFunctions.shannonEntropy(rowRDD, 1)
    val entropy3 = EntropyRddFunctions.shannonEntropy(rowRDD, 2)

    // Expected values were computed using the entropy.empirical method in the R entropy package
    // Input to entropy.empirical is an array of counts of distinct values
    entropy1 should equal(1.329661 +- epsilon) //entropy.empirical(c(1, 2, 2, 1), 'log')
    entropy2 should equal(1.011404 +- epsilon) //entropy.empirical(c(2,3,1), 'log')
    entropy3 should equal(0)
  }
  "shannonEntropy" should "compute the correct shannon entropy for weighted data" in {
    val rowRDD = sparkContext.parallelize(weightedInput, 2)
    val column = Column("columnName", DataTypes.float64)
    column.index = 1
    val entropy = EntropyRddFunctions.shannonEntropy(rowRDD, 0, Some(column))

    // Expected values were computed using the entropy.empirical method in the R entropy package
    // Input to entropy.empirical is an array of sums of weights of distinct values
    entropy should equal(0.891439 +- epsilon) //entropy.empirical(c(2, 1.1, 0.3), 'log')
  }
  "shannonEntropy" should "should return zero if frame is empty" in {
    val frameRdd = sparkContext.parallelize(emptyList, 2)
    val entropy = EntropyRddFunctions.shannonEntropy(frameRdd, 0)
    entropy should equal(0)
  }
}
