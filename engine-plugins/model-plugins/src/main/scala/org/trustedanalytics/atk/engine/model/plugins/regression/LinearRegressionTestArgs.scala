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
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Arguments to Linear Regression test plugin
 * @param model The trained linear regression model to run test on
 * @param frame The frame to test the linear regression model on
 * @param valueColumn Frame's column containing the value for the observation
 * @param observationColumns Frame's column(s) containing the observations
 */
case class LinearRegressionTestArgs(model: ModelReference,
                                    @ArgDoc("""The frame to test the linear regression model on""") frame: FrameReference,
                                    @ArgDoc("""Column name containing the value of each observation""") valueColumn: String,
                                    @ArgDoc("""List of column(s) containing the observations""") observationColumns: Option[List[String]])

/**
 * Return of Linear Regression test plugin
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param r2 The coefficient of determination
 * @param rootMeanSquaredError The square root of the mean squared error
 */
case class LinearRegressionTestReturn(@ArgDoc("""The explained variance regression score""") explainedVariance: Double,
                                      @ArgDoc("""The risk function corresponding to the expected value of the absolute error loss or l1-norm loss""") meanAbsoluteError: Double,
                                      @ArgDoc("""The risk function corresponding to the expected value of the squared error loss or quadratic loss""") meanSquaredError: Double,
                                      @ArgDoc("""The unadjusted coefficient of determination""") r2: Double,
                                      @ArgDoc("""The square root of the mean squared error""") rootMeanSquaredError: Double)