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

package org.trustedanalytics.atk.scoring.models

import com.cloudera.sparkts.models.ARIMAModel
import org.apache.spark.mllib.linalg.DenseVector
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelMetaDataArgs, Field }

class ARIMAScoreModel(arimaModel: ARIMAModel, arimaData: ARIMAData) extends ARIMAModel(arimaModel.p, arimaModel.d, arimaModel.q, arimaModel.coefficients, arimaModel.hasIntercept)
    with Model {

  /**
   * Predicts future values using ARIMA Model
   * @param data Array that includes: the time series values to use as the gold standard,
   *            followed by an integer value for the number of future periods to forecast
   *            (beyond the length of the specified time series).
   * @return Predicted values
   */
  override def score(data: Array[Any]): Array[Any] = {
    if (data.length < 2)
      throw new RuntimeException(s"Unexpected data length (${data.length.toString}). At least 2 values are required.")

    // This socring model only supports the scoring engine v2, and expects that the data array passed in contains:
    //  (0) a List[Double] of time series values
    //  (1) an integer for the number of future values to forecast
    if (data.length != 2)
      throw new IllegalArgumentException(s"Unexpected number of elements in the data array.  The ARIMA score model expects 2 elements, but received ${data.length}")

    if (data(0).isInstanceOf[List[Double]] == false)
      throw new IllegalArgumentException(s"The ARIMA score model expects the first item in the data array to be a List[Double].  Instead received ${data(0).getClass.getSimpleName}.")

    val timeseries = new DenseVector(data(0).asInstanceOf[List[Double]].map(ScoringModelUtils.asDouble(_)).toArray)
    val futurePeriods = ScoringModelUtils.asInt(data(1))

    data :+ forecast(timeseries, futurePeriods).toArray
  }

  override def input(): Array[Field] = {
    Array[Field](Field("timeseries", "Array[Double]"), Field("future", "Int"))
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("ARIMA Model", classOf[ARIMAModel].getName, classOf[ARIMAModelReaderPlugin].getName, Map())
  }

  override def output(): Array[Field] = {
    Array[Field](Field("predicted_values", "Array[Double]"))
  }

}
