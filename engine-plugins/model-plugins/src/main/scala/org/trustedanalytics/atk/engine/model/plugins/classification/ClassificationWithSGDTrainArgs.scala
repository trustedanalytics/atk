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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for loading model data into existing model in the model database.
 */
case class ClassificationWithSGDTrainArgs(model: ModelReference,
                                          @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                          @ArgDoc("""Column name containing the label
for each observation.""") labelColumn: String,
                                          @ArgDoc("""List of column(s) containing the
observations.""") observationColumns: List[String],
                                          @ArgDoc("""Flag indicating if the algorithm adds an intercept.
Default is true.""") intercept: Boolean = true,
                                          @ArgDoc("""Number of iterations for SGD. Default is 100.""") numIterations: Int = 100,
                                          @ArgDoc("""Initial step size for SGD optimizer for the first step.
Default is 1.0.""") stepSize: Double = 1.0,
                                          @ArgDoc("""Regularization "L1" or "L2".
Default is "L2".""") regType: Option[String] = None,
                                          @ArgDoc("""Regularization parameter. Default is 0.01.""") regParam: Double = 0.01,
                                          @ArgDoc("""Set fraction of data to be used for each SGD iteration. Default is 1.0; corresponding to deterministic/classical gradient descent.""") miniBatchFraction: Double = 1.0) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
  require(numIterations > 0, "numIterations must be a positive value")

}
