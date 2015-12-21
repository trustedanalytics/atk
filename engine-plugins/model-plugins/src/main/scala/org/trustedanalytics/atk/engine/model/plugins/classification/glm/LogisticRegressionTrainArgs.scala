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

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Input arguments for logistic regression train plugin
 */
case class LogisticRegressionTrainArgs(model: ModelReference,
                                       @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                       @ArgDoc("""Column name containing the label for each
observation.""") labelColumn: String,
                                       @ArgDoc("""Column(s) containing the
observations.""") observationColumns: List[String],
                                       @ArgDoc("""Optional column containing the frequency of
observations.""") frequencyColumn: Option[String] = None,
                                       @ArgDoc("""Number of classes""") numClasses: Int = 2,
                                       @ArgDoc("""Set type of optimizer.
| LBFGS - Limited-memory BFGS.
| LBFGS supports multinomial logistic regression.
| SGD - Stochastic Gradient Descent.
| SGD only supports binary logistic regression.""") optimizer: String = "LBFGS",
                                       @ArgDoc("""Compute covariance matrix for the
model.""") computeCovariance: Boolean = true,
                                       @ArgDoc("""Add intercept column to training
data.""") intercept: Boolean = true,
                                       @ArgDoc("""Perform feature scaling before training
model.""") featureScaling: Boolean = false,
                                       //TODO: Check if threshold needs to be set in both train() and predict
                                       @ArgDoc("""Threshold for separating positive predictions from
negative predictions.""") threshold: Double = 0.5,
                                       @ArgDoc("""Set type of regularization
| L1 - L1 regularization with sum of absolute values of coefficients
| L2 - L2 regularization with sum of squares of coefficients""") regType: String = "L2",
                                       @ArgDoc("""Regularization parameter""") regParam: Double = 0,
                                       //TODO: What input type should this be?
                                       //gradient : Option[Double] = None,
                                       @ArgDoc("""Maximum number of iterations""") numIterations: Int = 100,
                                       @ArgDoc("""Convergence tolerance of iterations for L-BFGS.
Smaller value will lead to higher accuracy with the cost of more
iterations.""") convergenceTolerance: Double = 0.0001,
                                       @ArgDoc("""Number of corrections used in LBFGS update.
Default is 10.
Values of less than 3 are not recommended;
large values will result in excessive computing time.""") numCorrections: Int = 10,
                                       @ArgDoc("""Fraction of data to be used for each SGD
iteration""") miniBatchFraction: Double = 1d,
                                       @ArgDoc("""Initial step size for SGD.
In subsequent steps, the step size decreases by stepSize/sqrt(t)""") stepSize: Double = 1d) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(optimizer == "LBFGS" || optimizer == "SGD", "optimizer name must be 'LBFGS' or 'SGD'")
  require(numClasses > 1, "number of classes must be greater than 1")
  if (optimizer == "SGD") require(numClasses == 2, "multinomial logistic regression not supported for SGD")
  require(observationColumns != null && observationColumns.nonEmpty, "observation columns must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "label column must not be null nor empty")
  require(numIterations > 0, "number of iterations must be a positive value")
  require(regType == "L1" || regType == "L2", "regularization type must be 'L1' or 'L2'")
  require(convergenceTolerance > 0, "convergence tolerance for LBFGS must be a positive value")
  require(numCorrections > 0, "number of corrections for LBFGS must be a positive value")
  require(miniBatchFraction > 0, "mini-batch fraction for SGD must be a positive value")
  require(stepSize > 0, "step size for SGD must be a positive value")

}

