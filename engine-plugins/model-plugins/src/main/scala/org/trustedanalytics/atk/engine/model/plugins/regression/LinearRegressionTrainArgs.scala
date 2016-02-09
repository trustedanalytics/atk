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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for loading model data into existing model in the model database.
 */
case class LinearRegressionTrainArgs(model: ModelReference,
                                     @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                     @ArgDoc("""Column name containing the label
for each observation.""") labelColumn: String,
                                     @ArgDoc("""List of column(s) containing the
observations.""") observationColumns: List[String],
                                     @ArgDoc("""Parameter for the ElasticNet mixing""") elasticNetParameter: Double = 0.0,
                                     @ArgDoc("""Parameter for whether to fit an intercept term""") fitIntercept: Boolean = true,
                                     @ArgDoc("""Parameter for maximum number of iterations""") maxIterations: Int = 100,
                                     @ArgDoc("""Parameter for regularization""") regParam: Double = 0.0,
                                     @ArgDoc("""Parameter for whether to standardize the training features before fitting the model""") standardization: Boolean = true,
                                     @ArgDoc("""Parameter for the convergence tolerance for iterative algorithms""") tolerance: Double = 1E-6) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
  require(maxIterations > 0, "numIterations must be a positive value")
  require(regParam >= 0, "regParam should be greater than or equal to 0")

}

case class LinearRegressionTrainReturn(@ArgDoc("""The list of column(s) storing the observations""") observationColumns: List[String],
                                       @ArgDoc("""Name of the column storing the label""") label: String,
                                       @ArgDoc("""Intercept of the trained model""") intercept: Double,
                                       @ArgDoc("""Weights of the trained model""") weights: Array[Double],
                                       @ArgDoc("""The explained variance regression score""") explainedVariance: Double,
                                       @ArgDoc("""The risk function corresponding to the expected value of the absolute error loss or l1-norm loss""") meanAbsoluteError: Double,
                                       @ArgDoc("""The risk function corresponding to the expected value of the squared error loss or quadratic loss""") meanSquaredError: Double,
                                       @ArgDoc("""Objective function(scaled loss + regularization) at each iteration""") objectiveHistory: Array[Double],
                                       @ArgDoc("""The coefficient of determination of the trained model""") r2: Double,
                                       @ArgDoc("""The square root of the mean squared error""") rootMeanSquaredError: Double,
                                       @ArgDoc("""The number of training iterations until termination""") iterations: Int)
