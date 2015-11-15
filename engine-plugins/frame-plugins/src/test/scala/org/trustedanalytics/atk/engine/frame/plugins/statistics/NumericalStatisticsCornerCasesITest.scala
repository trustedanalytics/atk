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
import org.scalatest.Assertions
import org.trustedanalytics.atk.engine.frame.plugins.statistics.numericalstatistics.NumericalStatistics
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class NumericalStatisticsCornerCasesITest extends TestingSparkContextFlatSpec with Matchers {

  /**
   * Tests some of the corner cases of the numerical statistics routines: NaNs, infinite values, divide by 0,
   * logarithms of negative numbers, etc. Floating point exceptions, if you catch my drift.
   */
  trait NumericalStatisticsCornerCaseTest {

    val epsilon = 0.000000001
  }

  "values with non-positive weights" should "be ignored , except for calculating count of non-positive weight data " in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, 8).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val dataFrequenciesWithNegs =
      sparkContext.parallelize((data :+ 1000.toDouble :+ (-10000).toDouble).zip(frequencies :+ (-1).toDouble :+ 0.toDouble)
        .map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)
    val numericalStatisticsWithNegs = new NumericalStatistics(dataFrequenciesWithNegs, false)

    numericalStatistics.positiveWeightCount shouldBe data.length
    numericalStatisticsWithNegs.positiveWeightCount shouldBe data.length

    Math.abs(numericalStatistics.weightedMean - numericalStatisticsWithNegs.weightedMean) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean
      - numericalStatisticsWithNegs.weightedGeometricMean) should be < epsilon
    Math.abs(numericalStatistics.min - numericalStatisticsWithNegs.min) should be < epsilon
    Math.abs(numericalStatistics.max - numericalStatisticsWithNegs.max) should be < epsilon
    Math.abs(numericalStatistics.weightedVariance - numericalStatisticsWithNegs.weightedVariance) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation
      - numericalStatisticsWithNegs.weightedStandardDeviation) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceLower
      - numericalStatisticsWithNegs.meanConfidenceLower) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper
      - numericalStatisticsWithNegs.meanConfidenceUpper) should be < epsilon

    numericalStatistics.nonPositiveWeightCount shouldBe 0
    numericalStatisticsWithNegs.nonPositiveWeightCount shouldBe 2

  }

  "values with NaN or infinite weights" should "be ignored , except for calculating count of bad rows " in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, 8).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val goodDataBadWeights = (1 to 3).map(x => x.toDouble).zip(List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity))
    val badDataGoodWeights = List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).zip((1 to 3).map(x => x.toDouble))

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val dataFrequenciesWithBadGuys =
      sparkContext.parallelize((data.zip(frequencies) ++ badDataGoodWeights ++ goodDataBadWeights).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)
    val numericalStatisticsWithBadGuys = new NumericalStatistics(dataFrequenciesWithBadGuys, false)

    numericalStatistics.positiveWeightCount shouldBe data.length
    numericalStatisticsWithBadGuys.positiveWeightCount shouldBe data.length

    numericalStatistics.badRowCount shouldBe 0
    numericalStatistics.goodRowCount shouldBe data.length

    numericalStatisticsWithBadGuys.badRowCount shouldBe 6
    numericalStatisticsWithBadGuys.goodRowCount shouldBe data.length

    Math.abs(numericalStatistics.weightedMean - numericalStatisticsWithBadGuys.weightedMean) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean
      - numericalStatisticsWithBadGuys.weightedGeometricMean) should be < epsilon
    Math.abs(numericalStatistics.min - numericalStatisticsWithBadGuys.min) should be < epsilon
    Math.abs(numericalStatistics.max - numericalStatisticsWithBadGuys.max) should be < epsilon
    Math.abs(numericalStatistics.weightedVariance - numericalStatisticsWithBadGuys.weightedVariance) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation
      - numericalStatisticsWithBadGuys.weightedStandardDeviation) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceLower
      - numericalStatisticsWithBadGuys.meanConfidenceLower) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper
      - numericalStatisticsWithBadGuys.meanConfidenceUpper) should be < epsilon

    numericalStatistics.nonPositiveWeightCount shouldBe 0
    numericalStatisticsWithBadGuys.nonPositiveWeightCount shouldBe 0

  }
  "when a data value is negative" should "give a NaN geometric mean" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, -18).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.weightedGeometricMean.isNaN() shouldBe true

  }

  "a data value is 0" should "give a NaN geometric mean " in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 0, 7, 18).map(x => x.toDouble)
    val frequencies = List(3, 2, 3, 1, 9, 4, 3, 1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.weightedGeometricMean.isNaN() shouldBe true

  }

  "empty data" should "provide expected statistics" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List()
    val frequencies: List[Double] = List()

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.positiveWeightCount shouldBe 0
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 0) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min.isNaN() shouldBe true
    numericalStatistics.max.isNaN() shouldBe true
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true
  }

  "all data has negative weights" should "be like empty data but with correct non-positive count" in new NumericalStatisticsCornerCaseTest() {

    val data = List(1, 2, 3, 4, 5, 6, 7, -18).map(x => x.toDouble)
    val frequencies = List(-3, -2, -3, -1, -9, -4, -3, -1).map(x => x.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.positiveWeightCount shouldBe 0
    numericalStatistics.nonPositiveWeightCount shouldBe data.length
    Math.abs(numericalStatistics.weightedMean - 0) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min.isNaN() shouldBe true
    numericalStatistics.max.isNaN() shouldBe true
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true

  }

  "data of length 1" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble)
    val frequencies: List[Double] = List(1.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.positiveWeightCount shouldBe 1
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 1) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 1
    numericalStatistics.weightedVariance.isNaN() shouldBe true
    numericalStatistics.weightedStandardDeviation.isNaN() shouldBe true
    numericalStatistics.meanConfidenceLower.isNaN() shouldBe true
    numericalStatistics.meanConfidenceUpper.isNaN() shouldBe true
  }

  "data of length 2" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 2.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 1.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.positiveWeightCount shouldBe 2
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 1.5) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - Math.sqrt(2)) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 2
    Math.abs(numericalStatistics.weightedVariance - 0.5) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation - Math.sqrt(0.5)) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceLower - (1.5 - 1.96 * Math.sqrt(0.5) / Math.sqrt(2))) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper - (1.5 + 1.96 * Math.sqrt(0.5) / Math.sqrt(2))) should be < epsilon
  }

  "data of length 3" should "work" in new NumericalStatisticsCornerCaseTest() {

    val data: List[Double] = List(1.toDouble, 2.toDouble, 3.toDouble)
    val frequencies: List[Double] = List(1.toDouble, 1.toDouble, 1.toDouble)

    val dataFrequencies = sparkContext.parallelize(data.zip(frequencies).map { case (k, v) => (Option(k), Option(v)) })

    val numericalStatistics = new NumericalStatistics(dataFrequencies, false)

    numericalStatistics.positiveWeightCount shouldBe 3
    numericalStatistics.nonPositiveWeightCount shouldBe 0
    Math.abs(numericalStatistics.weightedMean - 2.0) should be < epsilon
    Math.abs(numericalStatistics.weightedGeometricMean - 1.817120593) should be < epsilon
    numericalStatistics.min shouldBe 1
    numericalStatistics.max shouldBe 3
    Math.abs(numericalStatistics.weightedVariance - 1.0) should be < epsilon
    Math.abs(numericalStatistics.weightedStandardDeviation - 1.0) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceLower - (2.0 - 1.96 * (1.0 / Math.sqrt(3)))) should be < epsilon
    Math.abs(numericalStatistics.meanConfidenceUpper - (2.0 + 1.96 * (1.0 / Math.sqrt(3)))) should be < epsilon
  }

}
