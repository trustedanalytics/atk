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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.schema.{DataTypes, Column, FrameSchema}
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

  "extractCoxPointsWithMetaData in CoxCostFun with 0 beta" should "compute correct Rdd" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 2)

    val currentBeta = BDV(0d)
    val coxCostFun = new CoxCostFun(coxRdd)
    val coxWithMetaData = coxCostFun.extractCoxPointsWithMetaData(coxRdd, currentBeta)
    val coxWithMetaDataArray = coxWithMetaData.collect()

    val estimatedCoxMetaDataArray = Array(new CoxPointWithMetaData(new DenseVector(Array(27.9)), 2421.0, 1.0, 1.0, BDV(27.9), 1.0, BDV(27.9), 778.41),
      CoxPointWithMetaData(new DenseVector(Array(28.3)), 2201.0, 1.0, 2.0, BDV(28.3), 1.0, BDV(56.2), 1579.3),
      CoxPointWithMetaData(new DenseVector(Array(26.5)), 2065.0, 1.0, 3.0, BDV(26.5), 1.0, BDV(82.7), 2281.55),
      CoxPointWithMetaData(new DenseVector(Array(30.7)), 1205.0, 1.0, 4.0, BDV(30.7), 1.0, BDV(113.4), 3224.04),
      CoxPointWithMetaData(new DenseVector(Array(35.7)), 1002.0, 1.0, 5.0, BDV(35.7), 1.0, BDV(149.10000000000002), 4498.530000000001),
      CoxPointWithMetaData(new DenseVector(Array(22.7)), 374.0, 1.0, 6.0, BDV(22.7), 1.0, BDV(171.8), 5013.82),
      CoxPointWithMetaData(new DenseVector(Array(27.1)), 189.0, 1.0, 7.0, BDV(27.1), 1.0, BDV(198.9), 5748.2300000000005),
      CoxPointWithMetaData(new DenseVector(Array(21.5)), 98.0, 1.0, 8.0, BDV(21.5), 1.0, BDV(220.4), 6210.4800000000005),
      CoxPointWithMetaData(new DenseVector(Array(31.4)), 6.0, 1.0, 9.0, BDV(31.4), 1.0, BDV(251.8), 7196.4400000000005))

    coxWithMetaDataArray shouldBe estimatedCoxMetaDataArray
  }

  "extractCoxPointsWithMetaData in CoxCostFun with -0.0326" should "compute correct Rdd" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 2)

    val currentBeta = BDV(-0.0326)
    val coxCostFun = new CoxCostFun(coxRdd)
    val coxWithMetaData = coxCostFun.extractCoxPointsWithMetaData(coxRdd, currentBeta)
    val coxWithMetaDataArray = coxWithMetaData.collect()

    val estimatedCoxMetaDataArray = Array(
      new CoxPointWithMetaData(new DenseVector(Array(27.9)), 2421d, 1d, 0.4027094277702852, BDV(11.235593034790956), 0.4027094277702852, BDV(11.235593034790956), 313.47304567066766),
      new CoxPointWithMetaData(new DenseVector(Array(28.3)), 2201d, 1d, 0.8002016149399473, BDV(11.249028896901438), 0.3974921871696621, BDV(22.484621931692395), 631.8205634529784),
      new CoxPointWithMetaData(new DenseVector(Array(26.5)), 2065d, 1d, 1.2217165791047768, BDV(11.170146550367985), 0.4215149641648296, BDV(33.654768482060376), 927.82944703773),
      new CoxPointWithMetaData(new DenseVector(Array(30.7)), 1205d, 1d, 1.5892944827817275, BDV(11.284641642882386), 0.3675779036769507, BDV(44.93941012494276), 1274.2679454742192),
      new CoxPointWithMetaData(new DenseVector(Array(35.7)), 1002d, 1d, 1.9015854308015716, BDV(11.148786844308441), 0.31229094801984425, BDV(56.0881969692512), 1672.2796358160306),
      new CoxPointWithMetaData(new DenseVector(Array(22.7)), 374d, 1d, 2.378689804139718, BDV(10.830269274775917), 0.47710437333814615, BDV(66.91846624402712), 1918.1267483534439),
      new CoxPointWithMetaData(new DenseVector(Array(27.1)), 189d, 1d, 2.792040046893404, BDV(11.201791578624901), 0.41335024275368637, BDV(78.12025782265202), 2221.695300134179),
      new CoxPointWithMetaData(new DenseVector(Array(21.5)), 98d, 1d, 3.288178624968128, BDV(10.666979428606561), 0.4961385780747238, BDV(88.78723725125857), 2451.0353578492195),
      new CoxPointWithMetaData(new DenseVector(Array(31.4)), 6d, 1d, 3.647463385532477, BDV(11.281541481720554), 0.3592847605643489, BDV(100.06877873297913), 2805.275760375245))

    coxWithMetaDataArray shouldBe estimatedCoxMetaDataArray
  }
  "calculate in CoxCostFun" should "compute loss and gradient" in {
    val coxRdd = sparkContext.parallelize(sortedCoxPointArray, 2)

    val currentBeta = BDV(0d)
    val estimatedLoss = -12.801827480081469
    val estimatedGradient = BDV(-2.5120634920635005)
    val estimatedInformationMatrix = -77.12552113882519

    val coxCostFun = new CoxCostFun(coxRdd)
    val (loss, gradient, informationMatrix) = coxCostFun.calculate(currentBeta)

    loss shouldBe estimatedLoss +- 1e-6
    gradient.toArray should equalWithTolerance(estimatedGradient.toArray)
    informationMatrix shouldBe estimatedInformationMatrix +- 1e-6
  }

  "predict" should "compute predicted hazard ratio" in {
    val features = new DenseVector(Array(27.9))
    val estimatedPredictedOutput = 1.00261043677

    val coxModel = new CoxModel("coxModelId",beta=new DenseVector(Array(-0.03351902788328871)), meanVector = new DenseVector(Array(27.977777777777778)))
    val predictedOutput = coxModel.predict(features, coxModel.meanVector)

    predictedOutput shouldBe estimatedPredictedOutput +- 1e-6
  }

}
