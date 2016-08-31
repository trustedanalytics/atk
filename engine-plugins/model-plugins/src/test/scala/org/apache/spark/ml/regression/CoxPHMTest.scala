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
import breeze.linalg.{ DenseMatrix => BDM, * }

class CoxPHMTest extends TestingSparkContextFlatSpec with Matchers {

  val sortedCoxPointArray = Array(new CoxPoint(new DenseVector(Array(18d, 42d)), 6d, 1d),
    new CoxPoint(new DenseVector(Array(19d, 79d)), 5d, 1d),
    new CoxPoint(new DenseVector(Array(6d, 46d)), 4d, 1d),
    new CoxPoint(new DenseVector(Array(4d, 66d)), 3d, 1d),
    new CoxPoint(new DenseVector(Array(0d, 90d)), 2d, 1d),
    new CoxPoint(new DenseVector(Array(12d, 20d)), 1d, 1d),
    new CoxPoint(new DenseVector(Array(0d, 73d)), 0d, 1d))

  "extractCoxPointsWithMetaData in CoxCostFun with 0 beta" should "compute correct Rdd" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 3)

    val currentBeta = BDV(0d, 0d)
    val coxCostFun = new CoxCostFun(coxRdd)
    val coxWithMetaData = coxCostFun.extractCoxPointsWithMetaData(coxRdd, currentBeta)
    val coxWithMetaDataArray = coxWithMetaData.collect()

    val estimatedCoxMetaDataArray = Array(
      new CoxPointWithMetaData(new DenseVector(Array(18d, 42d)), 6d, 1d, 1d, BDV(18d, 42d), 1d, BDV(18d, 42d), new BDM(2, 2, Array(324.0, 756.0, 756.0, 1764.0))),
      new CoxPointWithMetaData(new DenseVector(Array(19d, 79d)), 5d, 1d, 2d, BDV(19d, 79d), 1d, BDV(37d, 121d), new BDM(2, 2, Array(685d, 2257d, 2257d, 8005d))),
      new CoxPointWithMetaData(new DenseVector(Array(6d, 46d)), 4d, 1d, 3d, BDV(6d, 46d), 1d, BDV(43d, 167d), new BDM(2, 2, Array(721d, 2533d, 2533d, 10121d))),
      new CoxPointWithMetaData(new DenseVector(Array(4d, 66d)), 3d, 1d, 4d, BDV(4d, 66d), 1d, BDV(47d, 233d), new BDM(2, 2, Array(737d, 2797d, 2797d, 14477d))),
      new CoxPointWithMetaData(new DenseVector(Array(0d, 90d)), 2d, 1d, 5d, BDV(0d, 90d), 1d, BDV(47d, 323d), new BDM(2, 2, Array(737d, 2797d, 2797d, 22577d))),
      new CoxPointWithMetaData(new DenseVector(Array(12d, 20d)), 1d, 1d, 6d, BDV(12d, 20d), 1d, BDV(59d, 343d), new BDM(2, 2, Array(881d, 3037d, 3037d, 22977d))),
      new CoxPointWithMetaData(new DenseVector(Array(0d, 73d)), 0d, 1d, 7d, BDV(0d, 73d), 1d, BDV(59d, 416d), new BDM(2, 2, Array(881d, 3037d, 3037d, 28306d))))
    coxWithMetaDataArray shouldBe estimatedCoxMetaDataArray
  }

  "computeGradientVector" should "compute correct Gradient vector" in {
    val data = new CoxPointWithMetaData(new DenseVector(Array(4d, 66d)), 3d, 1d, 4d, BDV(4d, 66d), 1d, BDV(47d, 233d), new BDM(2, 2, Array(737d, 2797d, 2797d, 14477d)))
    val estimatedGradientVector = BDV(-7.75, 7.75)

    val coxAgg = new CoxAggregator(BDV(0.0, 0.0))
    val computedGradientVector = coxAgg.computeGradientVector(data)

    computedGradientVector.toArray should equalWithTolerance(estimatedGradientVector.toArray)
  }

  "computeInformationMatrix" should "compute Information Matrix" in {
    val data = new CoxPointWithMetaData(new DenseVector(Array(0d, 73d)), 0d, 1d, 7d, BDV(0d, 73d), 1d, BDV(59d, 416d), new BDM(2, 2, Array(881d, 3037d, 3037d, 28306d)))
    val estimatedInformationMatrix = BDM(-54.816326, 67.040816, 67.040816, -511.959183)

    val coxAgg = new CoxAggregator(BDV(0.0, 0.0))
    val computedInformationMatrix = coxAgg.computeInformationMatrix(data)

    computedInformationMatrix.toArray should equalWithTolerance(estimatedInformationMatrix.toArray)
  }

  "calculate in CoxCostFun" should "compute loss and gradient" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 4)

    val currentBeta = BDV(0d, 0d)
    val estimatedLoss = -8.52516136
    val estimatedGradient = BDV(-31.24523, 18.38809)
    val estimatedInformationMatrix = BDM(-245.321604, 100.3460941, 100.3460941, -2258.997794)

    val coxCostFun = new CoxCostFun(coxRdd)
    val (loss, gradient, informationMatrix) = coxCostFun.calculate(currentBeta)

    loss shouldBe estimatedLoss +- 1e-6
    gradient.toArray should equalWithTolerance(estimatedGradient.toArray)
    informationMatrix.toArray should equalWithTolerance(estimatedInformationMatrix.toArray)
  }

}