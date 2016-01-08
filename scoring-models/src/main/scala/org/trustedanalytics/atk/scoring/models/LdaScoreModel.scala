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

import org.apache.spark.mllib.ScoringJsonReaderWriters
import org.trustedanalytics.atk.scoring.interfaces.Model
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import spray.json._
import ScoringJsonReaderWriters._

/**
 * Scoring model for Latent Dirichlet Allocation
 */
class LdaScoreModel(ldaModel: LdaModel) extends LdaModel(ldaModel.numTopics, ldaModel.topicWordMap) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    var score = Array[Any]()
    val predictReturn = predict(data.map(_.toString)toList)
    score = data :+ predictReturn.toJson
    score
  }

  override def input(): Array[Field] = {
    //TODO
    var input = Array[Field]()
    input
  }

  override def output(): Array[Field] = {
    var output = input()
    output = output :+ Field("topicsGivenDoc", "Vector[Double]")
    output = output :+ Field("newWordsCount", "Int")
    output:+ Field("percentOfNewWords", "Double")
  }

}
