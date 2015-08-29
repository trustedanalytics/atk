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

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.giraph.config.lda.LdaModelPredictReturn

import scala.collection.immutable.Map

/**
 * Model for Latent Dirichlet Allocation
 *
 * @param numTopics Number of topics in trained model
 * @param topicWordMap Map of conditional probabilities of topics given word
 */
case class LdaModel(numTopics: Int,
                    topicWordMap: Map[String, Vector[Double]]) {
  require(numTopics > 0, "number of topics must be greater than zero")

  /**
   * Predict conditional probabilities of topics given document
   *
   * @param document Test document represented as a list of words
   * @return Topic predictions for document
   */
  def predict(document: List[String]): LdaModelPredictReturn = {
    require(document != null, "document must not be null")

    val docLength = document.length
    val wordOccurrences: Map[String, Int] = computeWordOccurrences(document)
    val topicGivenDoc = new Array[Double](numTopics)

    for (word <- wordOccurrences.keys) {
      val wordGivenDoc = wordProbabilityGivenDocument(word, wordOccurrences, docLength)
      if (topicWordMap.contains(word)) {
        val topicGivenWord = topicWordMap(word)
        for (i <- topicGivenDoc.indices) {
          topicGivenDoc(i) += topicGivenWord(i) * wordGivenDoc
        }
      }
    }

    val newWordCount = computeNewWordCount(document)
    val percentOfNewWords = computeNewWordPercentage(newWordCount, docLength)
    new LdaModelPredictReturn(topicGivenDoc.toVector, newWordCount, percentOfNewWords)
  }

  /**
   * Compute counts for each word
   *
   * @param document Test document represented as a list of words
   * @return Map with counts for each word
   */
  def computeWordOccurrences(document: List[String]): Map[String, Int] = {
    require(document != null, "document must not be null")
    var wordOccurrences: Map[String, Int] = Map[String, Int]()
    for (word <- document) {
      val count = wordOccurrences.getOrElse(word, 0) + 1
      wordOccurrences += (word -> count)
    }
    wordOccurrences
  }

  /**
   * Compute conditional probability of word given document
   *
   * @param word Input word
   * @param wordOccurrences Number of occurrences of word in document
   * @param docLength Total number of words in document
   * @return Conditional probability of word given document
   */
  def wordProbabilityGivenDocument(word: String,
                                   wordOccurrences: Map[String, Int],
                                   docLength: Int): Double = {
    require(docLength >= 0, "number of words in document must be greater than or equal to zero")
    val wordCount = wordOccurrences.getOrElse(word, 0)
    if (docLength > 0) wordCount.toDouble / docLength else 0d
  }

  /**
   * Compute conditional probability of topic given word
   */
  def topicProbabilityGivenWord(word: String, topicIndex: Int): Double = {
    if (topicWordMap.contains(word)) {
      topicWordMap(word)(topicIndex)
    }
    else 0d
  }

  /**
   * Compute count of new words in document not present in trained model
   *
   * @param document Test document
   * @return Count of new words in document
   */
  def computeNewWordCount(document: List[String]): Int = {
    require(document != null, "document must not be null")
    var count = 0
    for (word <- document) {
      if (!topicWordMap.contains(word))
        count += 1
    }
    count
  }

  /**
   * Compute percentage of new words in document not present in trained model
   *
   * @param newWordCount Count of new words in document
   * @param docLength Total number of words in document
   * @return  Count of new words in document
   */
  def computeNewWordPercentage(newWordCount: Int, docLength: Int): Double = {
    require(docLength >= 0, "number of words in document must be greater than or equal to zero")
    if (docLength > 0) newWordCount * 100 / docLength.toDouble else 0d
  }
}

object LdaModel {

  /**
   * Create LDA model from frame
   *
   * @param topicsGivenWord Frame with conditional probabilities of topics given word
   * @param wordColumnName Name of column with words
   * @param topicProbColumnName Name of column with topic probabilities
   * @param numTopics Number of topics in trained model
   * @return LDA model
   */
  def createLdaModel(topicsGivenWord: FrameRdd,
                     wordColumnName: String,
                     topicProbColumnName: String,
                     numTopics: Int): LdaModel = {
    val topicWordMap = topicsGivenWord.mapRows(row => {
      val word = row.value(wordColumnName).toString
      val prob = row.vectorValue(topicProbColumnName)
      (word, prob)
    }).collect().toMap

    new LdaModel(numTopics, topicWordMap)
  }
}
