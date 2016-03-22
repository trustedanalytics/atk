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

import com.cloudera.sparkts.models.ARIMAModel
import org.apache.spark.mllib.linalg.DenseVector
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ Invocation, ApiMaturityTag, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.scoring.models.ARIMAData

// implicits needed for json conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAJsonProtocol._

/* Provided fitted values for timeseries ts as 1-step ahead forecasts, based on current
* model parameters, and then provide `nFuture` periods of forecast. We assume AR terms
* prior to the start of the series are equal to the model's intercept term (or 0.0, if fit
* without and intercept term).Meanwhile, MA terms prior to the start are assumed to be 0.0. If
* there is differencing, the first d terms come from the original series.

a series consisting of fitted 1-step ahead forecasts for historicals and then
*         `nFuture` periods of forecasts. Note that in the future values error terms become
*         zero and prior predictions are used for any AR terms.
*/

@PluginDoc(oneLine = """Forecasts future periods using ARIMA.""",
  extended =
    """Provided fitted values of the time series as 1-step ahead forecasts, based
on current model parameters, then provide future periods of forecast.  We assume
AR terms prior to the start of the series are equal to the model's intercept term
(or 0.0, if fit without an intercept term).  Meanwhile, MA terms prior to the start
are assumed to be 0.0.  If there is differencing, the first d terms come from the
original series.""",
  returns =
    """A series of 1-step ahead forecasts for historicals and then future periods
of forecasts.""")
class ARIMAPredictPlugin extends SparkCommandPlugin[ARIMAPredictArgs, ARIMAPredictReturn] {

  /**
   * The name of the command
   */
  override def name: String = "model:arima/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Get the predictions for observations in a test frame.
   *
   * @param arguments user supplied arguments used for ARIMA predict
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ARIMAPredictArgs)(implicit invocation: Invocation): ARIMAPredictReturn = {
    val model: Model = arguments.model

    // Extract the ARIMAModel from the stored JsObject
    val arimaData = model.data.convertTo[ARIMAData]
    val arimaModel = arimaData.arimaModel

    // Call ARIMA model to forecast values using the specified golden values
    val forecasted = arimaModel.forecast(new DenseVector(arguments.timeseriesValues.toArray), arguments.futurePeriods).toArray

    new ARIMAPredictReturn(forecasted)
  }
}
