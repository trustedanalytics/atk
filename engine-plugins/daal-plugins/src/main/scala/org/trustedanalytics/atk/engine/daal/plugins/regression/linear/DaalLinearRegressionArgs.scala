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
package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/** Json conversion for arguments and return value case classes */
object DaalLinearRegressionJsonFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val lrTrainFormat = jsonFormat5(DaalLinearRegressionTrainArgs)
  implicit val lrTrainResultFormat = jsonFormat9(DaalLinearRegressionTrainReturn)
  implicit val lrPredict = jsonFormat3(DaalLinearRegressionPredictArgs)
}

/**
 * Arguments for training and scoring DAAL linear regression model
 *
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param observationColumns Handle to the observation column of the data frame
 */
case class DaalLinearRegressionTrainArgs(model: ModelReference,
                                         @ArgDoc("""A frame to train or test the model on.""") frame: FrameReference,
                                         @ArgDoc("""Column name containing the value for each observation.""") valueColumn: String,
                                         @ArgDoc("""List of column(s) containing the observations.""") observationColumns: List[String],
                                         @ArgDoc("""Parameter for whether to fit an intercept term. Default is true""") fitIntercept: Boolean = true) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(valueColumn != null && !valueColumn.isEmpty, "valueColumn must not be null nor empty")
}

/**
 * Results of training DAAL linear regression model
 *
 * @param observationColumns Frame's column(s) storing the observations
 * @param valueColumn Frame's column storing the value of the observation
 * @param intercept The intercept of the trained model
 * @param weights Weights of the trained model
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param r2 The coefficient of determination of the trained model
 * @param rootMeanSquaredError The square root of the mean squared error
 */
case class DaalLinearRegressionTrainReturn(@ArgDoc("""The list of column(s) storing the observations""") observationColumns: List[String],
                                           @ArgDoc("""Name of column storing the value for each observation""") valueColumn: String,
                                           @ArgDoc("""Intercept of the trained model""") intercept: Double,
                                           @ArgDoc("""Weights of the trained model""") weights: Array[Double],
                                           @ArgDoc("""The explained variance regression score""") explainedVariance: Double,
                                           @ArgDoc("""The risk function corresponding to the expected value of the absolute error loss or l1-norm loss""") meanAbsoluteError: Double,
                                           @ArgDoc("""The risk function corresponding to the expected value of the squared error loss or quadratic loss""") meanSquaredError: Double,
                                           @ArgDoc("""The coefficient of determination of the trained model""") r2: Double,
                                           @ArgDoc("""The square root of the mean squared error""") rootMeanSquaredError: Double)

/**
 * Arguments for LinearRegression predict
 * @param model The trained Linear Regression model
 * @param frame The handle to the frame to run predict on
 * @param observationColumns The frame's column(s) storing the observations
 */
case class DaalLinearRegressionPredictArgs(model: ModelReference,
                                           @ArgDoc("""The frame to predict on""") frame: FrameReference,
                                           @ArgDoc("""List of column(s) containing the observations""") observationColumns: Option[List[String]])