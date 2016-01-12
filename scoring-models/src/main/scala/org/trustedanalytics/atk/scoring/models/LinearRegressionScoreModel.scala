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

import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.trustedanalytics.atk.scoring.interfaces.Model

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class LinearRegressionScoreModel(linearRegressionModel: LinearRegressionModel, linearRegressionData: LinearRegressionData) extends LinearRegressionModel(linearRegressionModel.weights, linearRegressionModel.intercept) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    val x: Array[Double] = data.map(y => ScoringModelUtils.toDouble(y))
    data :+ predict(Vectors.dense(x))
  }

  /**
   *  @return fields containing the input names and their datatypes
   */
  override def input(): Array[Field] = {
    val obsCols = linearRegressionData.observationColumns
    var input = Array[Field]()
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  /**
   *  @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("Prediction", "Double")
  }
}
