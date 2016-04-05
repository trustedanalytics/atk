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

import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }
import breeze.linalg.{ norm, DenseVector => BreezeDenseVector }

/**
 * Scoring model for Intel DAAL linear regression using QR decomposition
 *
 * @param lrData trained model
 */
class DaalLinearRegressionScoreModel(lrData: DaalLinearRegressionModelData) extends Model {
  val breezeWeights = new BreezeDenseVector[Double](lrData.weights)

  override def score(data: Array[Any]): Array[Any] = {
    val features: Array[Double] = data.map(y => ScoringModelUtils.asDouble(y))
    val breezeFeatures = new BreezeDenseVector[Double](features)
    val prediction = breezeWeights.dot(breezeFeatures) + lrData.intercept
    data :+ (prediction)
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def input(): Array[Field] = {
    var input = Array[Field]()
    val obsCols = lrData.observationColumns
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  /**
   * @return MEtadata for Intel DAAL linear regression scoring model
   */
  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs(
      "Intel DAAL Linear Regression Model",
      classOf[DaalLinearRegressionScoreModel].getName,
      classOf[DaalLinearRegressionReaderPlugin].getName,
      Map())
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("score", "Double")
  }
}
