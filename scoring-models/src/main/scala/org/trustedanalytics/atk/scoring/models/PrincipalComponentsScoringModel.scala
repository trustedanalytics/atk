/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.scoring.models

import breeze.linalg
import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.trustedanalytics.atk.scoring.interfaces.Model
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class PrincipalComponentsScoringModel(pcaModel: PrincipalComponentsData) extends PrincipalComponentsData(pcaModel.k, pcaModel.observationColumns,
  pcaModel.meanCentered, pcaModel.meanVector, pcaModel.singularValues, pcaModel.vFactor) with Model {

  override def score(data: Seq[Array[String]]): Future[Seq[Any]] = future {
    var score = Seq[Any]()
    data.foreach { row =>
      {
        val x: Array[Double] = new Array[Double](row.length)
        row.zipWithIndex.foreach {
          case (value: Any, index: Int) => x(index) = value.toDouble
        }
        val y: DenseMatrix = computePrincipalComponents(x)
        score = score :+ y
        val t_squared_index = computeTSquaredIndex(y.values, pcaModel.singularValues, pcaModel.k)
        score = score :+ t_squared_index
      }
    }
    score
  }

  def computePrincipalComponents(x: Array[Double]): DenseMatrix = {
    var inputVector = new org.apache.spark.mllib.linalg.DenseVector(x)
    if (pcaModel.meanCentered) {
      val meanCenteredVector: Array[Double] = (new linalg.DenseVector(x) - new linalg.DenseVector(pcaModel.meanVector.toArray)).toArray
      inputVector = new org.apache.spark.mllib.linalg.DenseVector(meanCenteredVector)
    }
    new DenseMatrix(1, inputVector.size, inputVector.toArray).multiply(pcaModel.vFactor.asInstanceOf[DenseMatrix])
  }

  def computeTSquaredIndex(y: Array[Double], E: Vector, k: Int): Double = {
    val yArray: Array[Double] = y
    var t: Double = 0.0
    for (i <- 0 to k - 1) {
      t += (yArray(i) * yArray(i)) / (E(i) * E(i))
    }
    t
  }
}

