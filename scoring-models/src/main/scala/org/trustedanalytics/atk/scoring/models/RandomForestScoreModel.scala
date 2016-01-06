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
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors

/**
 * Scoring model for MLLib's RandomForest
 * @param randomForestClassifierModel RandomForestModel(val algo: Algo.Algo, override val trees: Array[DecisionTreeModel])
 */
class RandomForestScoreModel(randomForestClassifierModel: RandomForestModel, randomForestData: RandomForestClassifierData) extends RandomForestModel(randomForestClassifierModel.algo, randomForestClassifierModel.trees) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    var score = Array[Any]()

    val x: Array[Double] = new Array[Double](data.length)
    data.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = value.asInstanceOf[Double]
    }
    score = data :+ predict(Vectors.dense(x))
    score
  }

  override def input: Array[Field] = {
    val obsCols = randomForestData.observationColumns
    val input: Array[Field] = new Array[Field](obsCols.length)
    var i = 0
    obsCols.foreach { obsColName =>
      {
        input(i) = new Field(obsColName)
        i = i + 1
      }
    }
    input
  }

  override def output: Array[Field] = {
    val obsCols = randomForestData.observationColumns
    val input: Array[Field] = new Array[Field](obsCols.length)
    var i = 0
    obsCols.foreach { obsColName =>
      {
        input(i) = new Field(obsColName)
        i = i + 1
      }
    }
    input
  }
}
