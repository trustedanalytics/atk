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

case class LinearRegressionTestArgs(model: ModelReference,
                                    @ArgDoc("""The frame to train the model on""") frame: FrameReference,
                                    @ArgDoc("""Column name containing the label of each observation""") labelColumn: String,
                                    @ArgDoc("""List of column(s) containing the observations""") observationColumns: Option[List[String]])

case class LinearRegressionTestReturn(@ArgDoc("""The variance explained by regression""") explainedVariance: Double,
                                      @ArgDoc("""The risk function corresponding to the expected value of the absolute error loss or l1-norm loss""") meanAbsoluteError: Double,
                                      @ArgDoc("""The risk function corresponding to the expected value of the squared error loss or quadratic loss""") meanSquaredError: Double,
                                      @ArgDoc("""The unadjusted coefficient of determination""") r2: Double,
                                      @ArgDoc("""The square root of the mean squared error""") rootMeanSquaredError: Double)