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

package org.trustedanalytics.atk.engine.model.plugins.classification.glm

object LogisticRegressionModelWrapperFactory {

  /**
   * Create logistic regression model based on optimizer.
   *
   * The optimizers supported are: LBFGS, and SGD
   *
   * @param arguments Model training arguments
   * @return Logistic regression model
   */
  def createModel(arguments: LogisticRegressionTrainArgs): LogisticRegressionModelWrapper = {
    val regressionModel = arguments.optimizer.toUpperCase() match {
      case "LBFGS" => new LogisticRegressionModelWrapperWithLBFGS(arguments)
      case "SGD" => new LogisticRegressionModelWrapperWithSGD(arguments)
      case _ => throw new IllegalArgumentException("Only LBFGS or SGD optimizers permitted")
    }
    regressionModel
  }

}
