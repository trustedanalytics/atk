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
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ Invocation, ApiMaturityTag, SparkCommandPlugin, PluginDoc }
import com.cloudera.sparkts.models.{ ARIMA, ARIMAModel }
import org.trustedanalytics.atk.scoring.models.ARIMAData
import scala.collection.mutable.ArrayBuffer
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }

// implicits needed for json conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAJsonProtocol._

@PluginDoc(oneLine = "Creates Autoregressive Integrated Moving Average (ARIMA) Model from train frame.",
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
    val frame: SparkFrame = arguments.frame
    val model = arguments.model
    var trainFrame = frame.rdd

    // Verify that the specified time series column exists and is a vector
    frame.schema.requireColumnIsType(arguments.timeseriesColumn, DataTypes.isVectorDataType)

    // Only train one model at a time (for a single key)
    if (frame.rdd.count() != 1)
      throw new RuntimeException("Unexpected number of rows in time series frame.  Expected 1 row, but the frame has " + frame.rdd.count.toString + " rows.")

    // Call fitModel() for each row (there should be just one row)
    val arimaModels = frame.rdd.mapRows(row => {
      val timeseriesValues = row.vectorValue(arguments.timeseriesColumn)
      ARIMA.fitModel(arguments.p, arguments.d, arguments.q, new DenseVector(timeseriesValues.toArray),
        arguments.includeIntercept, arguments.method)
    })

    // Verify that we have just one item, and then grab it as an ARIMA Model.
    if (arimaModels.count() != 1)
      throw new RuntimeException("Exepcted 1 ARIMA model from training, but recevied " + arimaModels.count.toString + " models.")
    val arimaModel = arimaModels.collect()(0).asInstanceOf[ARIMAModel]

    val userInitParams = if (arguments.userInitParams.isDefined) arguments.userInitParams.get.toArray else Array[Double](0)
    val jsonModel = new ARIMAData(arimaModel)
    model.data = jsonModel.toJson.asJsObject

    new ARIMATrainReturn(arimaModel.coefficients)
  }
}
