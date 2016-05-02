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

package org.trustedanalytics.atk.engine.model.plugins.survivalanalysis

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Cox proportional model train arguments
 */
case class MultivariateCoxTrainArgs(model: ModelReference,
                                    @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                                    @ArgDoc("""Column containing the time data.""") timeColumn: String,
                                    @ArgDoc("""List of column(s) containing the covariate data.""") covariateColumns: List[String],
                                    @ArgDoc("""List of column(s) containing the censored data. Can have 2 values: 0 - event did not happen (censored); 1 - event happened (not censored)""") censorColumn: String,
                                    @ArgDoc("""Convergence tolerance""") convergenceTolerance: Double = 1E-6,
                                    //@ArgDoc("""Initial beta.""") initialBetas: List[Double] = 0.0,
                                    @ArgDoc("""Max steps.""") maxSteps: Int = 100) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(timeColumn), "Event time column name must be provided")
  //require(StringUtils.isNotBlank(covariateColumns), "covarianceColumn must not be null nor empty")
  require(maxSteps > 0, "Max steps must be a positive integer")
}

/**
 * Return object when training a CoxProportionalHazardModel
 * @param beta beta at final step
 */
case class MultivariateCoxTrainReturn(beta: List[Double])
