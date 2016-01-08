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

import org.trustedanalytics.atk.scoring.interfaces.Model
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

class KMeansScoreModel(libKMeansModel: KMeansModel, kmeansData: KMeansData) extends KMeansModel(libKMeansModel.clusterCenters) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    var score = Array[Any]()
    val x: Array[Double] = data.map(y => ScoringModelUtils.toDouble(y))
    score = data :+ (predict(Vectors.dense(x)) + 1)
    score
  }

  override def input(): Array[Field] = {
    var input = Array[Field]()
    val obsCols = kmeansData.observationColumns
    obsCols.foreach { name =>
      input = input :+ Field(name)
    }
    input
  }

  override def output(): Array[Field] = {
    var output = input()
    //Int
    output :+ Field("score")
  }
}
