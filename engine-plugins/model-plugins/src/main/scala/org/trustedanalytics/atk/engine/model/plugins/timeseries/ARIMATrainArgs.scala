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

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Arguments used for training an ARIMA Model
 */
case class ARIMATrainArgs(model: ModelReference,
                          @ArgDoc("""List of time series values.""") timeseriesValues: List[Double],
                          @ArgDoc("""Autoregressive order""") p: Int,
                          @ArgDoc("""Differencing order""") d: Int,
                          @ArgDoc("""Moving average order""") q: Int,
                          @ArgDoc("If true, the model is fit with an intercept.  Default is True") includeIntercept: Boolean = true,
                          @ArgDoc("""Objective function and optimization method.  Current options are: 'css-bobyqa'
           and 'css-cgd'.  Both optimize the log likelihood in terms of the conditional sum of
           squares.  The first uses BOBYQA for optimization, while the second uses conjugate
           gradient descent.  Default is 'css-cgd'.""") method: String = "css-cgd",
                          @ArgDoc("""A set of user provided initial parameters for optimization. If the list
            is empty (default), initialized using Hannan-Rissanen algorithm. If provided, order of parameter
            should be: intercept term, AR parameters (in increasing order of lag), MA parameters (in
            increasing order of lag)""") userInitParams: Option[List[Double]] = None) {
  require(model != null, "model must not be null")
  require(timeseriesValues != null && timeseriesValues.nonEmpty, "List of time series values must not be null or empty.")
}

/**
 * Return object when training an ARIMAModel
 * @param coefficients intercept, AR, MA, with increasing degrees
 */
case class ARIMATrainReturn(coefficients: Array[Double])