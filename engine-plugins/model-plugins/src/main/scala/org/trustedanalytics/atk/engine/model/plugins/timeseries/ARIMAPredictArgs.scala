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

import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Parameters used for predicting future values using the ARIMA Model
 */
case class ARIMAPredictArgs(model: ModelReference,
                            @ArgDoc("""Number of periods in the future to forecast (beyond the length the time series)""") futurePeriods: Int,
                            @ArgDoc(
                              """Optional list of time series values to use as the gold standard. If no values are
                                |provided, the same values that were used during training will be used for forecasting.
                              """.stripMargin) timeseriesValues: Option[List[Double]]) {
  require(model != null, "model is required")
  require(futurePeriods >= 0, "Number of future periods should be greater than or equal to 0.")
}

/**
 * Return object when forecasting values using an ARIMAModel
 * @param forecasted a series consisting of fitted 1-step ahead forecasts for historicals and then
 *         the number of future periods of forecasts. Note that in the future values error terms become
 *         zero and prior predictions are used for any AR terms.
 */
case class ARIMAPredictReturn(forecasted: Array[Double])