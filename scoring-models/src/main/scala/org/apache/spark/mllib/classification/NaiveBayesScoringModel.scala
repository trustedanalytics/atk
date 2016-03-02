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
package org.apache.spark.mllib.classification

import org.apache.spark.mllib.linalg.Vectors
import org.trustedanalytics.atk.scoring.interfaces.{ ModelMetaDataArgs, Model, Field }
import org.trustedanalytics.atk.scoring.models.{ ScoringModelUtils, NaiveBayesReaderPlugin, NaiveBayesData }

class NaiveBayesScoringModel(naiveBayesData: NaiveBayesData) extends NaiveBayesModel(naiveBayesData.naiveBayesModel.labels, naiveBayesData.naiveBayesModel.pi, naiveBayesData.naiveBayesModel.theta) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    val x: Array[Double] = new Array[Double](data.length)
    val inputNames = input().map(f => f.name)
    val inputMap: Map[String, Any] = (inputNames zip data).toMap
    data.zipWithIndex.foreach {
      case (value: Any, index: Int) => x(index) = ScoringModelUtils.toDouble(value)
    }
    val prediction = predict(Vectors.dense(x))
    val scoreOutput: Map[String, Any] = Map(
      "prediction" -> prediction
    )
    val output: Array[Any] = Array(inputMap ++ scoreOutput)
    output
  }

  /**
   *  @return fields containing the input names and their datatypes
   */
  override def input(): Array[Field] = {
    var input = Array[Field]()
    val obsCols = naiveBayesData.observationColumns
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("Naive Bayes Model", classOf[NaiveBayesScoringModel].getName, classOf[NaiveBayesReaderPlugin].getName, Map())
  }

  /**
   *  @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    //Double
    output :+ Field("score", "Double")
  }
}

