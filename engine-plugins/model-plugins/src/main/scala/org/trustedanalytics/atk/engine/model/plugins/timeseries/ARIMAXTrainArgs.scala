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
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAXJsonProtocol._

/**
 * Command for loading model data into existing model in the model database.
 */
case class ARIMAXTrainArgs(model: ModelReference,
                           @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                           @ArgDoc("""Name of the column that contains the time series values.""") timeseriesColumn: String,
                           @ArgDoc("""Names of the column(s) that contain the values of previous exogenous regressors.""") xColumns: List[String],
                           @ArgDoc("""Autoregressive order""") p: Int,
                           @ArgDoc("""Differencing order""") d: Int,
                           @ArgDoc("""Moving average order""") q: Int,
                           @ArgDoc("""The maximum lag order for exogenous variables""") xregMaxLag: Int,
                           @ArgDoc("If true, the model is fit with an original exogenous variables (intercept for exogenous variables). Default is True") includeOriginalXreg: Boolean = true,
                           @ArgDoc("If true, the model is fit with an intercept.  Default is True") includeIntercept: Boolean = true,
                           @ArgDoc("""A set of user provided initial parameters for optimization. If the list is empty
(default), initialized using Hannan-Rissanen algorithm. If provided, order of parameter should be: intercept term, AR
parameters (in increasing order of lag), MA parameters (in increasing order of lag)""") userInitParams: Option[List[Double]] = None) {

  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(timeseriesColumn != null && timeseriesColumn.nonEmpty, "timeseriesColumn must not be null nor empty")
  require(xColumns != null && xColumns.nonEmpty, "Must provide at least one x column.")
}

/**
 * Return object when training an ARIMAXModel
 * @param c an intercept term
 * @param ar the coefficients for autoregressive terms for the dependent variable, in increasing order of lag
 * @param ma the coefficients for moving average terms for the dependent variable, in increasing order of lag
 * @param xreg the coefficients for each column in the exogenous matrix
 */
case class ARIMAXTrainReturn(c: Double, ar: Array[Double], ma: Array[Double], xreg: Array[Double])
