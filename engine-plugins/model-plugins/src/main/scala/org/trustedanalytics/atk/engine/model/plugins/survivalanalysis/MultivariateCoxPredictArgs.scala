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
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.engine.model.plugins.survivalanalysis

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class MultivariateCoxPredictArgs(model: ModelReference,
                                      @ArgDoc("A frame whose hazard values") predictFrame: FrameReference,
                                      @ArgDoc("A frame storing observations to compare hazards of the predict frame against. By default it is the frame used to train the model ")comparisonFrame: Option[FrameReference],
                                      @ArgDoc("Columns containing the observations. By default it is the columns used to train the model")featureColumns: Option[List[String]]) {
  require(model != null, "model is required")
  require(predictFrame != null, "frame is required")
}
