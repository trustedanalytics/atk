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
                                     elasticNetParameter: Double = 0.0,
                                     fitIntercept: Boolean = true,
                                     maxIterations: Int = 100,
                                     regParam: Double = 0.0,
                                     standardization: Boolean = true,
                                     tolerance: Double = 1E-6) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
  require(maxIterations > 0, "numIterations must be a positive value")

}

case class LinearRegressionTrainReturn(observationColumns: List[String], label: String, intercept: Double, weights: Array[Double], explainedVariance: Double,
                                       meanAbsoluteError: Double, meanSquaredError: Double, objectiveHistory: Array[Double], r2: Double,
                                       rootMeanSquaredError: Double, iterations: Int)
