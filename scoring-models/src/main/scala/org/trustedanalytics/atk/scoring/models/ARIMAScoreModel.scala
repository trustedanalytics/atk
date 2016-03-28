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

    var returnData = Array[Any]()

    // Check if the data array is a list of doubles (the golden values) and an int (number of future periods to predict).
    // If so, this call came from v2
    if (data(0).isInstanceOf[List[Double]] && ScoringModelUtils.isNumeric(data(1))) {
      val timeseries = new DenseVector(data(0).asInstanceOf[List[Double]].map(ScoringModelUtils.toDouble(_)).toArray)
      val futurePeriods = ScoringModelUtils.toInt(data(1))
      returnData = data :+ (forecast(timeseries, futurePeriods)).toArray
    }
    else {
      // Otherwise for v1, we just get a flat array of values (timeseries and the number of periods to forecast)
      val timeseries = new DenseVector(data.slice(0, data.length - 1).map(ScoringModelUtils.toDouble(_)))
      val futurePeriods = ScoringModelUtils.toInt(data(data.length - 1))
      returnData = data :+ List.fromArray(forecast(timeseries, futurePeriods).toArray)
    }

    returnData
  }

  override def input(): Array[Field] = {
    var input = Array[Field]()
    input = input :+ Field("timeseries", "Array[Double]")
    input :+ Field("future", "Int")
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("ARIMA Model", classOf[ARIMAModel].getName, classOf[ARIMAModelReaderPlugin].getName, Map())
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("predicted_values", "Array[Double]")
  }

}
