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

import breeze.linalg.{ DenseMatrix, DenseVector, diag }
import breeze.numerics.sqrt
import breeze.stats.distributions.ChiSquared
import org.apache.spark.mllib.classification.LogisticRegressionModelWithFrequency
import org.trustedanalytics.atk.domain.frame.FrameEntity

/**
 * Summary table with results of logistic regression train plugin
 *
 * @param numFeatures Number of features
 * @param numClasses Number of classes
 * @param coefficients Model coefficients
 *                     The dimension of the coefficients' vector is
 *                     (numClasses - 1) * (numFeatures + 1) if `addIntercept == true`, and
 *                     (numClasses - 1) * numFeatures if `addIntercept != true`
 * @param degreesFreedom Degrees of freedom for model coefficients
 * @param covarianceMatrix Optional covariance matrix
 * @param standardErrors Optional standard errors for model coefficients
 *                       The standard error for each variable is the square root of
 *                       the diagonal of the covariance matrix
 * @param waldStatistic Optional Wald Chi-Squared statistic
 *                      The Wald Chi-Squared statistic is the coefficients
 *                      divided by the standard errors
 * @param pValue Optional p-values for the model coefficients
 */
case class LogisticRegressionSummaryTable(numFeatures: Int,
                                          numClasses: Int,
                                          coefficients: Map[String, Double],
                                          degreesFreedom: Map[String, Double],
                                          covarianceMatrix: Option[FrameEntity] = None,
                                          standardErrors: Option[Map[String, Double]] = None,
                                          waldStatistic: Option[Map[String, Double]] = None,
                                          pValue: Option[Map[String, Double]] = None)

/**
 * Summary table builder
 *
 * @param logRegModel Logistic regression model
 * @param observationColumns Names of observation columns
 * @param isAddIntercept If true, intercept column was added to training data
 * @param hessianMatrix Optional Hessian matrix for trained model
 */
case class SummaryTableBuilder(logRegModel: LogisticRegressionModelWithFrequency,
                               observationColumns: List[String],
                               isAddIntercept: Boolean = true,
                               hessianMatrix: Option[DenseMatrix[Double]] = None) {
  require(logRegModel != null, "logistic regression model must not be null")
  require(observationColumns != null && observationColumns.nonEmpty, "list of observation columns must not be empty")

  val interceptName = "intercept"
  val coefficients = getCoefficients
  val coefficientNames = getCoefficientNames
  val approxCovarianceMatrix = getApproxCovarianceMatrix
  val degreesFreedom = getDegreesFreedom

  /**
   * Build summary table for trained model
   */
  def build(covarianceFrame: Option[FrameEntity] = None): LogisticRegressionSummaryTable = {
    approxCovarianceMatrix match {
      case Some(approxMatrix) =>
        val stdErrors = getStandardErrors(coefficients, approxMatrix.covarianceMatrix)
        val waldStatistic = getWaldStatistic(coefficients, stdErrors)
        val pValues = getPValues(waldStatistic)

        LogisticRegressionSummaryTable(
          logRegModel.numFeatures,
          logRegModel.numClasses,
          (coefficientNames zip coefficients.toArray).toMap,
          (coefficientNames zip degreesFreedom.toArray).toMap,
          covarianceFrame,
          Some((coefficientNames zip stdErrors.toArray).toMap),
          Some((coefficientNames zip waldStatistic.toArray).toMap),
          Some((coefficientNames zip pValues.toArray).toMap)
        )
      case _ => LogisticRegressionSummaryTable(
        logRegModel.numFeatures,
        logRegModel.numClasses,
        (coefficientNames zip coefficients.toArray).toMap,
        (coefficientNames zip degreesFreedom.toArray).toMap
      )
    }
  }

  /**
   * Compute the covariance matrix if the Hessian matrix is defined.
   */
  private def getApproxCovarianceMatrix: Option[ApproximateCovarianceMatrix] = {
    hessianMatrix match {
      case Some(hessian) =>
        val reorderMatrix = isAddIntercept && logRegModel.numClasses <= 2
        Some(ApproximateCovarianceMatrix(hessian, reorderMatrix))
      case _ => None
    }
  }

  /**
   * Return the model coefficients.
   *
   * The dimension of coefficients vector is (numClasses - 1) * (numFeatures + 1) if `addIntercept == true`,
   * and if `addIntercept != true`, the dimension will be (numClasses - 1) * numFeatures
   */
  private def getCoefficients: DenseVector[Double] = {
    if (isAddIntercept && logRegModel.numClasses <= 2) {
      DenseVector(logRegModel.intercept +: logRegModel.weights.toArray)
    }
    else {
      DenseVector(logRegModel.weights.toArray)
    }
  }

  /**
   * Return the names of the model coefficients.
   *
   * The names of the model coefficients correspond to the observation columns.
   * If the number of classes > 2, then each observation column name is repeated (numClasses - 1) times.
   * A suffix is added to the observation column name to differentiate repeated entries.
   */
  private def getCoefficientNames: List[String] = {
    val interceptCol = if (isAddIntercept) 1 else 0
    val names = if (logRegModel.numClasses > 2) {
      for {

        i <- 0 until (logRegModel.numClasses - 1)
        j <- 0 until (logRegModel.numFeatures + interceptCol)
        coefName = if (j < logRegModel.numFeatures) {
          s"${observationColumns(j)}_$i"
        }
        else {
          s"${interceptName}_$i"
        }
      } yield coefName
    }
    else {
      if (isAddIntercept) interceptName +: observationColumns else observationColumns
    }

    names.toList
  }

  /**
   * Return the degrees of freedom for model coefficients.
   */
  private def getDegreesFreedom: DenseVector[Double] = {
    DenseVector.fill(coefficients.length) {
      1
    }
  }

  /**
   * Return the standard errors for model coefficients.
   */
  private def getStandardErrors(coefficients: DenseVector[Double],
                                covarianceMatrix: DenseMatrix[Double]): DenseVector[Double] = {
    sqrt(diag(covarianceMatrix))
  }

  /**
   * Return the Wald Chi-Squared statistic for model coefficients.
   */
  private def getWaldStatistic(coefficients: DenseVector[Double], stdErrors: DenseVector[Double]): DenseVector[Double] = {
    coefficients :/ stdErrors //element-wise division
  }

  /**
   * Return the p-values for model coeffients.
   */
  private def getPValues(waldStatistic: DenseVector[Double]): DenseVector[Double] = {
    val chi = ChiSquared(1)
    waldStatistic.map(w => 1 - chi.cdf(w))
  }
}
