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

package org.apache.spark.ml.regression

import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.MatcherUtils._
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec
import breeze.linalg.{ DenseVector => BDV, * }

class CoxTest extends TestingSparkContextFlatSpec with Matchers {

  val sortedCoxPointArray = Array(new CoxPoint(new DenseVector(Array(27.9)), 2421d, 1d),
    new CoxPoint(new DenseVector(Array(28.3)), 2201d, 1d),
    new CoxPoint(new DenseVector(Array(26.5)), 2065d, 1d),
    new CoxPoint(new DenseVector(Array(30.7)), 1205d, 1d),
    new CoxPoint(new DenseVector(Array(35.7)), 1002d, 1.0),
    new CoxPoint(new DenseVector(Array(22.7)), 374d, 1.0),
    new CoxPoint(new DenseVector(Array(27.1)), 189d, 1d),
    new CoxPoint(new DenseVector(Array(21.5)), 98d, 1d),
    new CoxPoint(new DenseVector(Array(31.4)), 6d, 1d))

  "extractCoxPointsWithMetaData in CoxCostFun" should "compute correct Rdd" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 2)

    val currentBeta = BDV(0d)
    val coxCostFun = new CoxCostFun(coxRdd)
    val coxWithMetaData = coxCostFun.extractCoxPointsWithMetaData(coxRdd, currentBeta)
    val coxWithMetaDataArray = coxWithMetaData.collect()

    val estimatedCoxMetaDataArray = Array(new CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(27.9)), 2421.0, 1.0, 1.0, BDV(27.9), 1.0, BDV(27.9)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(28.3)), 2201.0, 1.0, 2.0, BDV(28.3), 1.0, BDV(56.2)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(26.5)), 2065.0, 1.0, 3.0, BDV(26.5), 1.0, BDV(82.7)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(30.7)), 1205.0, 1.0, 4.0, BDV(30.7), 1.0, BDV(113.4)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(35.7)), 1002.0, 1.0, 5.0, BDV(35.7), 1.0, BDV(149.10000000000002)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(22.7)), 374.0, 1.0, 6.0, BDV(22.7), 1.0, BDV(171.8)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(27.1)), 189.0, 1.0, 7.0, BDV(27.1), 1.0, BDV(198.9)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(21.5)), 98.0, 1.0, 8.0, BDV(21.5), 1.0, BDV(220.4)),
      CoxPointWithCumulativeSumAndBetaX(new DenseVector(Array(31.4)), 6.0, 1.0, 9.0, BDV(31.4), 1.0, BDV(251.8)))

    coxWithMetaDataArray shouldBe estimatedCoxMetaDataArray
  }

  "calculate in CoxCostFun" should "compute loss and gradient" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 2)

    val currentBeta = BDV(0d)
    val estimatedLoss = -1.4224252755646078
    val estimatedGradient = BDV(-0.2791181657848334)

    val coxCostFun = new CoxCostFun(coxRdd)
    val (loss, gradient) = coxCostFun.calculate(currentBeta)

    loss shouldBe estimatedLoss +- 1e-6
    gradient.toArray should equalWithTolerance(estimatedGradient.toArray)

  }
}
