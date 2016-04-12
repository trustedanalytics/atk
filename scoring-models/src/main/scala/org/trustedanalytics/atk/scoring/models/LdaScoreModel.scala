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

import org.apache.spark.mllib.ScoringJsonReaderWriters._
import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }
import spray.json._
import org.apache.spark.mllib.ScoringJsonReaderWriters

/**
 * Scoring model for Latent Dirichlet Allocation
 *
 * The input to the scoring model is a document represented as list of words,
 * and the output is the topic vector, and the number of new words not contained
 * in the original model.
 */
class LdaScoreModel(ldaModel: LdaModel) extends LdaModel(
  ldaModel.numTopics,
  ldaModel.topicWordMap,
  ldaModel.documentColumnName,
  ldaModel.wordColumnName) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    val inputDocument = data.flatMap {
      case list: List[_] => list.map(_.toString)
      case _ => throw new IllegalArgumentException("Scoring input must be a list of words")
    }
    val predictReturn = predict(inputDocument.toList)
    data :+ Array(predictReturn.topicsGivenDoc, predictReturn.newWordsCount, predictReturn.newWordsPercentage)
  }

  /**
   * Input for the LDA model is a document containing a list of words
   * @return fields containing the input names and their datatypes
   */
  override def input(): Array[Field] = {
    val input = Array[Field](Field(documentColumnName, "Array[String]"))
    input
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("Lda Model", classOf[LdaScoreModel].getName, classOf[LdaModelReaderPlugin].getName, Map())
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    output = output :+ Field("topics_given_doc", "Vector[Double]")
    output = output :+ Field("new_words_count", "Int")
    output :+ Field("new_words_percentage", "Double")
  }
}
