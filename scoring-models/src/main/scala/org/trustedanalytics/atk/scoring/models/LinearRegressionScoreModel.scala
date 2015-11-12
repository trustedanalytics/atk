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

class LinearRegressionScoreModel(linearRegressionModel: LinearRegressionModel) extends LinearRegressionModel(linearRegressionModel.weights, linearRegressionModel.intercept) with Model {

  override def score(data: Seq[Array[String]]): Seq[Any] = {
    var score = Seq[Any]()
    var value: Int = 2
    data.foreach { row =>
      {
        val x: Array[Double] = new Array[Double](row.length)
        row.zipWithIndex.foreach {
          case (value: Any, index: Int) => x(index) = value.toDouble
        }
        score = score :+ (predict(Vectors.dense(x)))
      }
    }
    score
  }

}
