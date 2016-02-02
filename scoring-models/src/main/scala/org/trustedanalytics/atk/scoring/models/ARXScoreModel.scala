/**
 *  Copyright (c) 2016 Intel Corporation 
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

import org.trustedanalytics.atk.scoring.interfaces.Model

import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg._
import com.cloudera.sparkts.ARXModel

class ARXScoreModel(arxModel: ARXModel, includesOriginalX: Boolean) extends ARXModel(arxModel.c, arxModel.coefficients, arxModel.yMaxLag, arxModel.xMaxLag, includesOriginalX) with Model {

  override def score(data: Seq[Array[String]]): Seq[Any] = {
    var score = Seq[Any]()
    //    data.foreach { row =>
    //    {
    //      val tempMatrix = Matrix.create(1,1, new Array[Double](1))
    //
    //      val y: Array[Double] = new Array[Double](row.length)
    //      val x = Array.ofDim[Double](row.length, 2)
    //      row.zipWithIndex.foreach {
    //        case (value: Any, index: Int) => y(index) = value.toDouble
    //      }
    //      score = score :+ (predict(Vectors.dense(y).asInstanceOf[Vector[Double]], tempMatrix) + 1)
    //    }
    //    }
    score
  }

}
