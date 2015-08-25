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

package org.trustedanalytics.atk.giraph.plugins.model.lda

import org.trustedanalytics.atk.domain.model.ModelReference

import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map

case class LdaModel(val topicWordMap: Map[String, Vector[Double]]) {

  def predict(document: List[String]): LdaModelPredictReturn = {
    val numberOfWords = document.length
    val numberOfTopics = topicWordMap.values.head.size
    val wordOccurrences: Map[String, Int] = computeWordOccurences(document)
    var topicsGivenDocumentBuffer = new ListBuffer[Double]()
    for (x <- 0 until numberOfTopics) {
      var topicGivenDocument: Double = 0.0
      for (word <- document) {
        val wGivenD = wordProbabilityGivenDocument(word, wordOccurrences, numberOfWords)
        val topicsProbablitiesForWord = topicWordMap(word)
        val topicProbabilityForWord = topicWordMap(word)(x)
        topicGivenDocument += topicProbabilityForWord * wGivenD
      }
      topicsGivenDocumentBuffer += topicGivenDocument
    }
    val newWords: Int = computeNewWords(topicWordMap, document)
    val percentOfNewWords: Double = newWords * 100 / numberOfWords
    new LdaModelPredictReturn(topicsGivenDocumentBuffer.toVector, newWords, percentOfNewWords)
  }

  def computeWordOccurences(document: List[String]): Map[String, Int] = {
    var wordOccurences: Map[String, Int] = Map[String, Int]()
    for (word <- document) {
      if (wordOccurences.contains(word)) {
        val count = wordOccurences(word) + 1
        wordOccurences += (word -> count)
      }
      else { wordOccurences += (word -> 1) }
    }
    wordOccurences
  }

  def wordProbabilityGivenDocument(word: String, wordOccurences: Map[String, Int], numberOfWords: Int): Double = {
    val wordCount = wordOccurences.getOrElse(word, 0)
    wordCount / numberOfWords
  }

  def computeNewWords(topicWordMap: Map[String, Vector[Double]], document: List[String]): Int = {
    var count = 0
    for (word <- document) {
      if (!topicWordMap.contains(word))
        count += 1
    }
    count
  }
}

case class LdaModelPredictReturn(topicsGivenDocumentVector: Vector[Double], newWordsCount: Int, percentageOfNewWords: Double)

case class LdaModelPredictArgs(model: ModelReference, document: List[String])