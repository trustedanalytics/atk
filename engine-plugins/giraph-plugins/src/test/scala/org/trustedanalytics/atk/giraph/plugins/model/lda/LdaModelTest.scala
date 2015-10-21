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

import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.MatcherUtils._

import scala.collection.immutable.Map

class LdaModelTest extends FlatSpec with Matchers {
  val epsilon = 1e-6
  val numTopics = 2

  val topicWordMap = Map(
    "jobs" -> Vector(0.15d, 0.85d),
    "harry" -> Vector(0.9d, 0.1d),
    "economy" -> Vector(0.35d, 0.65d)
  )

  val wordOccurrences = Map(
    "jobs" -> 5,
    "harry" -> 2,
    "economy" -> 8
  )

  "ldaModel" should "throw an IllegalArgumentException if number of topics is less than one" in {
    intercept[IllegalArgumentException] {
      val ldaModel = LdaModel(0, topicWordMap)
    }
  }

  "predict" should "compute topic probabilities for document" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)

    val document = List("jobs", "harry", "jobs", "harry", "harry", "new_word")
    val prediction = ldaModel.predict(document)
    prediction.topicsGivenDoc.toArray should equalWithTolerance(Array(0.5, 0.333333))
    prediction.newWordsCount should equal(1)
    prediction.newWordsPercentage should equal(100 / 6d +- epsilon)
  }

  "predict" should "compute topic probabilities for empty document" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)

    val document = List()
    val prediction = ldaModel.predict(document)
    prediction.topicsGivenDoc.toArray should equalWithTolerance(Array(0d, 0d))
    prediction.newWordsCount should equal(0)
    prediction.newWordsPercentage should equal(0d +- epsilon)
  }

  "computeWordCounts" should "compute counts of each word in document" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)
    val document = List("jobs", "harry", "jobs", "harry", "harry", "new_word")
    val wordCounts = ldaModel.computeWordOccurrences(document)

    wordCounts.size should equal(3)
    wordCounts("jobs") should equal(2)
    wordCounts("harry") should equal(3)
    wordCounts("new_word") should equal(1)

    ldaModel.computeWordOccurrences(List()).isEmpty should be(true)
  }

  "computeWordCounts" should "throw an IllegalArgumentException if document is null" in {
    intercept[IllegalArgumentException] {
      val ldaModel = LdaModel(numTopics, topicWordMap)
      ldaModel.computeWordOccurrences(null)
    }
  }

  "wordProbabilityGivenDocument" should "compute probability of word given document" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)

    ldaModel.wordProbabilityGivenDocument("jobs", wordOccurrences, 15) should equal(0.333333 +- epsilon)
    ldaModel.wordProbabilityGivenDocument("new_word", wordOccurrences, 15) should equal(0)
  }

  "computeNewWordCount" should "count words not present in trained model" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)

    ldaModel.computeNewWordCount(List("jobs", "harry", "economy", "new_word1", "new_word2")) should equal(2)
    ldaModel.computeNewWordCount(List("jobs", "harry", "new_word", "new_word", "new_word2")) should equal(3)
    ldaModel.computeNewWordCount(List("jobs", "harry", "economy")) should equal(0)
    ldaModel.computeNewWordCount(List()) should equal(0)
  }

  "computeNewWordCount" should "throw an IllegalArgumentException if document is null" in {
    intercept[IllegalArgumentException] {
      val ldaModel = LdaModel(numTopics, topicWordMap)
      ldaModel.computeNewWordCount(null) should equal(0)
    }
  }

  "computeNewWordPercentage" should "compute percentage of words not present in trained model" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)
    ldaModel.computeNewWordPercentage(3, 12) should equal(25d +- epsilon)
    ldaModel.computeNewWordPercentage(0, 0) should equal(0d +- epsilon)
  }
}
