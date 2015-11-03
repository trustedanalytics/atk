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


package org.trustedanalytics.atk.engine.frame.plugins.statistics

import org.scalatest.Matchers
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Tests the frequency statistics package through several corner cases and bad-data cases, as well as "happy path"
 * use cases with both normalized and un-normalized weights.
 */
class FrequencyStatisticsITest extends TestingSparkContextFlatSpec with Matchers {

  trait FrequencyStatisticsTest {

    val epsilon = 0.000000001

    val integers = (1 to 6) :+ 1 :+ 7 :+ 3

    val strings = List("a", "b", "c", "d", "e", "f", "a", "g", "c")

    val integerFrequencies = List(1, 1, 5, 1, 7, 2, 2, 3, 2).map(_.toDouble)

    val modeFrequency = 7.toDouble
    val totalFrequencies = integerFrequencies.sum

    val fractionalFrequencies: List[Double] = integerFrequencies.map(x => x / totalFrequencies)

    val modeSetInts = Set(3, 5)
    val modeSetStrings = Set("c", "e")

    val firstModeInts = Set(3)
    val firstModeStrings = Set("c")
    val maxReturnCount = 10

  }

  "empty data" should "produce mode == None and weights equal to 0" in new FrequencyStatisticsTest {

    val dataList: List[Double] = List()
    val weightList: List[Double] = List()

    val dataWeightPairs = sparkContext.parallelize(dataList.zip(weightList))

    val frequencyStats = new FrequencyStatistics[Double](dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet should be('empty)
    testModeWeight shouldBe 0
    testTotalWeight shouldBe 0
  }

  "integer data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetInts
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "string data with integer frequencies" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetStrings
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "integer data with integer frequencies" should "get least mode when asking for just one" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, 1)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe firstModeInts
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "string data with integer frequencies" should "get least mode when asking for just one" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(integerFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, 1)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe firstModeStrings
    testModeWeight shouldBe modeFrequency
    testTotalWeight shouldBe totalFrequencies
  }

  "integer data with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(integers.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetInts
    Math.abs(testModeWeight - (modeFrequency / totalFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon

  }

  "string data  with fractional weights" should "work" in new FrequencyStatisticsTest {

    val dataWeightPairs = sparkContext.parallelize(strings.zip(fractionalFrequencies))

    val frequencyStats = new FrequencyStatistics(dataWeightPairs, maxReturnCount)

    val testModeSet = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testModeSet shouldBe modeSetStrings
    Math.abs(testModeWeight - (modeFrequency / totalFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }

  "items with negative weights" should "not affect mode or total weight" in new FrequencyStatisticsTest {

    val dataWeightPairs: RDD[(String, Double)] =
      sparkContext.parallelize((strings :+ "haha").zip(fractionalFrequencies :+ -10.0))

    val frequencyStats = new FrequencyStatistics[String](dataWeightPairs, maxReturnCount)

    val testMode = frequencyStats.modeSet
    val testModeWeight = frequencyStats.weightOfMode
    val testTotalWeight = frequencyStats.totalWeight

    testMode shouldBe modeSetStrings
    Math.abs(testModeWeight - (modeFrequency / totalFrequencies)) should be < epsilon
    Math.abs(testTotalWeight - 1.toDouble) should be < epsilon
  }
}
