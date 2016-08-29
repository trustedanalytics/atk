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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAXJsonProtocol._

/**
 * Command for loading model data into existing model in the model database.
 */
case class ARIMAXPredictArgs(model: ModelReference,
                             @ArgDoc("""A frame whose values are to be predicted.""") frame: FrameReference,
                             @ArgDoc("""Name of the column that contains the time series values.""") timeseriesColumn: String,
                             @ArgDoc("""Names of the column(s) that contain the values of the exogenous inputs.""") xColumns: List[String]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(timeseriesColumn != null && timeseriesColumn.nonEmpty, "timeseriesColumn must not be null nor empty")
  require(xColumns != null && xColumns.nonEmpty, "Must provide at least one x column.")
}
