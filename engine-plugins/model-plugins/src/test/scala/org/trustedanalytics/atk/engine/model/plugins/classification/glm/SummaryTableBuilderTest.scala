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
package org.trustedanalytics.atk.engine.model.plugins.classification.glm

import breeze.linalg.{ DenseMatrix, inv }
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.apache.spark.mllib.linalg.DenseVector
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.domain.frame.FrameEntity

class SummaryTableBuilderTest extends FlatSpec with Matchers with MockitoSugar {

  val tolerance = 1e-6

  "SummaryTableBuilder" should "build summary table without an intercept column" in {
    val model = mock[LogisticRegressionModelWithFrequency]
    val obsColumns = List("col1", "col2", "col3")
    val weights = Array(1.5, 2.0d, 3.4d)

    when(model.numClasses).thenReturn(2)
    when(model.numFeatures).thenReturn(obsColumns.size)
    when(model.intercept).thenReturn(0d)
    when(model.weights).thenReturn(new DenseVector(weights))

    val summaryTableBuilder = SummaryTableBuilder(model, obsColumns, isAddIntercept = false)
    val interceptName = summaryTableBuilder.interceptName
    val summaryTable = summaryTableBuilder.build()

    summaryTable.numClasses shouldBe 2
    summaryTable.numFeatures shouldBe 3
    obsColumns.zipWithIndex.foreach { case (col, i) => summaryTable.coefficients(col) shouldBe weights(i) +- tolerance }
    obsColumns.zipWithIndex.foreach { case (col, i) => summaryTable.degreesFreedom(col) shouldBe 1d +- tolerance }

    summaryTable.coefficients.get(interceptName) shouldBe empty
    summaryTable.degreesFreedom.get(interceptName) shouldBe empty
    summaryTable.standardErrors shouldBe empty
    summaryTable.waldStatistic shouldBe empty
    summaryTable.pValue shouldBe empty
    summaryTable.covarianceMatrix shouldBe empty
  }

  "SummaryTableBuilder" should "build summary table with an intercept column" in {
    val inputCovMatrix = DenseMatrix(
      (2d, 1d, 0d),
      (2d, 4d, 0d),
      (2d, 0d, 1d)
    )
    val covarianceFrame = mock[FrameEntity]
    val hessianMatrix = inv(inputCovMatrix)

    //Reordered covariance matrix where the intercept is stored in the first row and first column
    //instead of in the last row and last column of the matrix
    val reorderedCovMatrix = DenseMatrix(
      (1d, 2d, 0d),
      (0d, 2d, 1d),
      (0d, 2d, 4d)
    )

    val model = mock[LogisticRegressionModelWithFrequency]
    val obsColumns = List("col1", "col2")
    val intercept = 4.5d
    val weights = Array(2.0d, 3.4d)

    when(model.numClasses).thenReturn(2)
    when(model.numFeatures).thenReturn(obsColumns.size)
    when(model.intercept).thenReturn(intercept)
    when(model.weights).thenReturn(new DenseVector(weights))

    val summaryTableBuilder = SummaryTableBuilder(model, obsColumns, isAddIntercept = true,
      hessianMatrix = Some(hessianMatrix))
    val interceptName = summaryTableBuilder.interceptName
    val summaryTable = summaryTableBuilder.build(Some(covarianceFrame))

    summaryTable.numClasses shouldBe 2
    summaryTable.numFeatures shouldBe 2

    val expectedCoefNames = interceptName +: obsColumns
    val expectedCoefs = intercept +: weights
    val expectedErrors = List(1d, 1.414214d, 2d) //square root of diagonal of covariance matrix
    val expectedWaldStats = List(4.5d, 1.414214d, 1.7d) //coefs divided by standard error

    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.coefficients(col) shouldBe expectedCoefs(i) +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.degreesFreedom(col) shouldBe 1d +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.standardErrors.get(col) shouldBe expectedErrors(i) +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.waldStatistic.get(col) shouldBe expectedWaldStats(i) +- tolerance
    }

    summaryTable.pValue shouldBe defined
    summaryTable.covarianceMatrix shouldBe defined
  }

  "SummaryTableBuilder" should "build summary table for multinomial logistic regression with intercept and covariance matrix" in {
    val covarianceFrame = mock[FrameEntity]

    val inputCovMatrix = DenseMatrix(
      (5d, 8d, -9d, 7d, 5d, 3d),
      (-0.05d, 6d, -0.25d, 4d, 4d, 7d),
      (0d, 0d, 3d, -2d, -5d, -1d),
      (-0.02d, -0.05d, -0.15d, 1d, -5d, 4d),
      (-0.05d, -0.09d, -0.22d, -0.13d, 1d, 6d),
      (1d, 2d, 5d, 3d, 2d, 1d)
    )
    val hessianMatrix = inv(inputCovMatrix)

    //If the number of classes > 2, then each observation variable occurs (numClasses - 1) times in the weights.
    val numClasses = 3
    val obsColumns = List("col1", "col2")
    val intercept = 0d
    val weights = Array(1.0, 2.5, 5.0, 7.5, 6, 3)

    val model = mock[LogisticRegressionModelWithFrequency]
    when(model.numClasses).thenReturn(numClasses)
    when(model.numFeatures).thenReturn(obsColumns.size)
    when(model.intercept).thenReturn(intercept)
    when(model.weights).thenReturn(new DenseVector(weights))

    val summaryTableBuilder = SummaryTableBuilder(model, obsColumns, hessianMatrix = Some(hessianMatrix))
    val interceptName = summaryTableBuilder.interceptName
    val summaryTable = summaryTableBuilder.build(Some(covarianceFrame))

    summaryTable.numClasses shouldBe numClasses
    summaryTable.numFeatures shouldBe obsColumns.size

    val expectedCoefNames = List("col1_0", "col2_0", interceptName + "_0",
      "col1_1", "col2_1", interceptName + "_1")
    val expectedCoefs = weights
    val expectedErrors = List(2.236068, 2.449490, 1.732051, 1, 1, 1) //square root of diagonal of covariance matrix
    val expectedWaldStats = List(0.4472136, 1.0206207, 2.8867513, 7.5, 6, 3) //coefs divided by standard error

    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.coefficients(col) shouldBe expectedCoefs(i) +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.degreesFreedom(col) shouldBe 1d +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.standardErrors.get(col) shouldBe expectedErrors(i) +- tolerance
    }
    expectedCoefNames.zipWithIndex.foreach {
      case (col, i) => summaryTable.waldStatistic.get(col) shouldBe expectedWaldStats(i) +- tolerance
    }

    summaryTable.pValue shouldBe defined
    summaryTable.covarianceMatrix shouldBe defined
  }

  "SummaryTableBuilder" should "throw an IllegalArgumentException if logistic regression model is null" in {
    intercept[IllegalArgumentException] {
      SummaryTableBuilder(null, List("a", "b"))
    }
  }

  "SummaryTableBuilder" should "throw an IllegalArgumentException if list of observation columns is null" in {
    intercept[IllegalArgumentException] {
      val model = mock[LogisticRegressionModelWithFrequency]
      SummaryTableBuilder(model, null)
    }
  }

  "SummaryTableBuilder" should "throw an IllegalArgumentException if list of observation columns is empty" in {
    intercept[IllegalArgumentException] {
      val model = mock[LogisticRegressionModelWithFrequency]
      SummaryTableBuilder(model, List.empty[String])
    }
  }
}
