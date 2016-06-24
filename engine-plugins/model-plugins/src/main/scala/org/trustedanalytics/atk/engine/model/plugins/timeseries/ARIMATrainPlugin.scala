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

import org.apache.spark.mllib.linalg.DenseVector
import org.trustedanalytics.atk.engine.plugin.{ Invocation, ApiMaturityTag, SparkCommandPlugin, PluginDoc }
import com.cloudera.sparkts.models.{ ARIMA }
import org.trustedanalytics.atk.scoring.models.ARIMAData

// implicits needed for json conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAJsonProtocol._

@PluginDoc(oneLine = "Creates Autoregressive Integrated Moving Average (ARIMA) Model from the specified time series values.",
  extended = """Given a time series, fits an non-seasonal Autoregressive Integrated Moving Average (ARIMA) model of
order (p, d, q) where p represents the autoregression terms, d represents the order of differencing,
and q represents the moving average error terms.  If includeIntercept is true, the model is fitted
with an intercept.""",
  returns = "Array of coefficients (intercept, AR, MA, with increasing degrees).")
class ARIMATrainPlugin extends SparkCommandPlugin[ARIMATrainArgs, ARIMATrainReturn] {
  /**
   * The name of the command
   */
  override def name: String = "model:arima/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  override def execute(arguments: ARIMATrainArgs)(implicit invocation: Invocation): ARIMATrainReturn = {
    val model = arguments.model

    // Fit model using the specified time series values and ARIMA parameters
    val userInitParams = if (arguments.userInitParams.isDefined) arguments.userInitParams.get.toArray else null
    val arimaModel = ARIMA.fitModel(arguments.p, arguments.d, arguments.q, new DenseVector(arguments.timeseriesValues.toArray),
      arguments.includeIntercept, arguments.method, userInitParams)

    val jsonModel = new ARIMAData(arimaModel, arguments.timeseriesValues)
    model.data = jsonModel.toJson.asJsObject

    new ARIMATrainReturn(arimaModel.coefficients)
  }
}
