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

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Command for loading model data into existing model in the model database.
 */
case class ARXTrainArgs(model: ModelReference,
                        @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                        @ArgDoc("""Name of the column that contains the time series values.""") timeseriesColumn: String,
                        @ArgDoc("""Names of the column(s) that contain the values of previous exogenous regressors.""") xColumns: List[String],
                        @ArgDoc("""The maximum lag order for the dependent (time series) variable""") yMaxLag: Int,
                        @ArgDoc("""The maximum lag order for exogenous variables""") xMaxLag: Int,
                        @ArgDoc("""a boolean flag indicating if the intercept should be dropped. Default is false""") noIntercept: Boolean = false) {

  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(timeseriesColumn != null && timeseriesColumn.nonEmpty, "timeseriesColumn must not be null nor empty")
  require(xColumns != null && xColumns.nonEmpty, "Must provide at least one x column.")
}

/**
 * Return object when training a ARXModel
 * @param c an intercept term, zero if none desired
 * @param coefficients the coefficients for the various terms. The order of coefficients is as
 *                     follows:
 *                     - Autoregressive terms for the dependent variable, in increasing order of lag
 *                     - For each column in the exogenous matrix (in their original order), the
 *                     lagged terms in increasing order of lag (excluding the non-lagged versions).
 *                     - The coefficients associated with the non-lagged exogenous matrix
 */
case class ARXTrainReturn(c: Double, coefficients: Array[Double])
