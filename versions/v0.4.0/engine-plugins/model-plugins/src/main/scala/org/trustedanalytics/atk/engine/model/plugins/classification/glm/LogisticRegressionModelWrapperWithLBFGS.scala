/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.trustedanalytics.atk.engine.model.plugins.classification.glm

import breeze.linalg.DenseMatrix
import org.apache.spark.mllib.classification.LogisticRegressionWithFrequencyLBFGS
import org.apache.spark.mllib.optimization.{ SquaredL2Updater, L1Updater }

/**
 * Logistic regression model with Limited-memory BFGS
 */
class LogisticRegressionModelWrapperWithLBFGS() extends LogisticRegressionModelWrapper {

  /** Logistic regression model */
  val model = new LogisticRegressionWithFrequencyLBFGS()

  /**
   * Create Logistic regression model with Limited-memory BFGS
   * @param arguments model arguments
   */
  def this(arguments: LogisticRegressionTrainArgs) = {
    this()
    model.setNumClasses(arguments.numClasses)
    model.setFeatureScaling(arguments.featureScaling)
    model.setIntercept(arguments.intercept)
    model.optimizer.setNumIterations(arguments.numIterations)
    model.optimizer.setConvergenceTol(arguments.convergenceTolerance)
    model.optimizer.setNumCorrections(arguments.numCorrections)
    model.optimizer.setRegParam(arguments.regParam)
    model.optimizer.setComputeHessian(arguments.computeCovariance)

    model.optimizer.setUpdater(arguments.regType match {
      case "L1" => new L1Updater()
      case other => new SquaredL2Updater()
    })
  }

  /**
   * Get logistic regression model
   */
  override def getModel: LogisticRegressionWithFrequencyLBFGS = model

  /**
   * Get the approximate Hessian matrix for the model
   */
  override def getHessianMatrix: Option[DenseMatrix[Double]] = {
    model.optimizer.getHessianMatrix
  }
}
