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

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.MatcherUtils._

class LdaScoringModelTest extends FlatSpec with Matchers with ScalaFutures {
  val epsilon = 1e-6
  val numTopics = 2

  val topicWordMap = Map(
    "jobs" -> Vector(0.15d, 0.85d),
    "harry" -> Vector(0.9d, 0.1d),
    "economy" -> Vector(0.35d, 0.65d)
  )

  "LdaScoringModel" should "throw an IllegalArgumentException if number of topics is less than one" in {
    intercept[IllegalArgumentException] {
      new LdaScoringModel(LdaModel(0, topicWordMap))
    }
  }

  "predict" should "compute topic probabilities for document" in {
    val ldaModel = LdaModel(numTopics, topicWordMap)
    val scoringModel = new LdaScoringModel(ldaModel)

    val documents = Seq(
      Array("jobs", "harry", "jobs", "harry", "harry", "new_word"),
      Array("jobs", "economy", "economy", "harry"),
      Array.empty[String]
    )

    val scores = scoringModel.score(documents).futureValue

    scores.size should equal(3)

    val score0 = scores(0).asInstanceOf[LdaModelPredictReturn]
    score0.topicsGivenDoc.toArray should equalWithTolerance(Array(0.5, 0.333333))
    score0.newWordsCount should equal(1)
    score0.newWordsPercentage should equal(100 / 6d +- epsilon)

    val score1 = scores(1).asInstanceOf[LdaModelPredictReturn]
    score1.topicsGivenDoc.toArray should equalWithTolerance(Array(0.4375, 0.5625))
    score1.newWordsCount should equal(0)
    score1.newWordsPercentage should equal(0d +- epsilon)

    val score2 = scores(2).asInstanceOf[LdaModelPredictReturn]
    score2.topicsGivenDoc.toArray should equalWithTolerance(Array(0d, 0d))
    score2.newWordsCount should equal(0)
    score2.newWordsPercentage should equal(0d +- epsilon)
  }

}
