/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.evaluation.RegressionMetrics

/**
 * Training summary for Intel DAAL linear regression model using QR decomposition
 *
 * @param observationColumns List of column(s) containing the observations
 * @param valueColumn Column name containing the value for each observation.
 * @param trainedModel Trained model
 * @param predictFrame Frame containing the original frame's columns and a column with the predicted value
 */
case class DaalLinearRegressionSummary(observationColumns: List[String],
                                       valueColumn: String,
                                       trainedModel: DaalLinearRegressionModel,
                                       predictFrame: FrameRdd) {

  /**
   * Compute training summary
   */
  def summarize(): DaalLinearRegressionTrainReturn = {
    val summary = getRegressionMetrics(valueColumn)
    val explainedVariance = summary.explainedVariance
    val meanAbsoluteError = summary.meanAbsoluteError
    val meanSquaredError = summary.meanSquaredError
    val r2 = summary.r2
    val rootMeanSquaredError = summary.rootMeanSquaredError

    DaalLinearRegressionTrainReturn(
      observationColumns,
      valueColumn,
      trainedModel.intercept,
      trainedModel.weights,
      explainedVariance,
      meanAbsoluteError,
      meanSquaredError,
      r2,
      rootMeanSquaredError)
  }

  //Get regression metrics for trained model
  private def getRegressionMetrics(valueColumn: String): RegressionMetrics = {

    val predictionAndObservations = predictFrame.mapRows(row => {
      val prediction = row.doubleValue(DaalLinearPredictAlgorithm.PredictColumnPrefix + valueColumn)
      val value = row.doubleValue(valueColumn)
      (prediction, value)
    })

    val summary = new RegressionMetrics(predictionAndObservations)
    summary
  }
}
