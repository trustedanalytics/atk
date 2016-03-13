/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Input arguments for principal components train plugin
 */
case class PrincipalComponentsTrainArgs(model: ModelReference,
                                        @ArgDoc("""A frame to train the model
on.""") frame: FrameReference,
                                        @ArgDoc("""List of column(s) containing
the observations.""") observationColumns: List[String],
                                        @ArgDoc("""Option to mean center the
columns""") meanCentered: Boolean = true,
                                        @ArgDoc("""Principal component count.
Default is the number of observation columns""") k: Option[Int] = None) {
  require(frame != null, "frame is required")
  require(!observationColumns.contains(null), "data columns names cannot be null")
  require(observationColumns.forall(!_.equals("")), "data columns names cannot be empty")
}
