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

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.classification.{ LogisticRegressionModelWithFrequency, LogisticRegressionWithFrequencySGD }
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }
import org.apache.spark.mllib.regression.GeneralizedLinearAlgorithmWithFrequency

/**
 * Logistic regression model with Stochastic Gradient Descent
 */
class LogisticRegressionModelWrapperWithSGD extends LogisticRegressionModelWrapper {

  val model = new LogisticRegressionWithFrequencySGD()

  /**
   * Create Logistic regression model with Stochastic Gradient Descent
   * @param arguments model arguments
   */
  def this(arguments: LogisticRegressionTrainArgs) = {
    this()
    model.setFeatureScaling(arguments.featureScaling)
    model.setIntercept(arguments.intercept)
    model.optimizer.setNumIterations(arguments.numIterations)
    model.optimizer.setStepSize(arguments.stepSize)
    model.optimizer.setRegParam(arguments.regParam)
    model.optimizer.setMiniBatchFraction(arguments.miniBatchFraction)
    model.optimizer.setComputeHessian(arguments.computeCovariance)

    model.optimizer.setUpdater(arguments.regType match {
      case "L1" => new L1Updater()
      case other => new SquaredL2Updater()
    })
  }

  /**
   * Get logistic regression model
   */
  override def getModel: GeneralizedLinearAlgorithmWithFrequency[LogisticRegressionModelWithFrequency] = model

  /**
   * Get the approximate Hessian matrix for the model
   */
  override def getHessianMatrix: Option[DenseMatrix[Double]] = {
    model.optimizer.getHessianMatrix
  }
}
