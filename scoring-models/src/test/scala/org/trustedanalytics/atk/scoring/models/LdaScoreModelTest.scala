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

import org.apache.spark.mllib.ScoringJsonReaderWriters
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FlatSpec, Matchers }
import ScoringJsonReaderWriters._
import org.trustedanalytics.atk.testutils.MatcherUtils._
import spray.json._

class LdaScoreModelTest extends FlatSpec with Matchers with ScalaFutures {
  val epsilon = 1e-6
  val numTopics = 2

  val topicWordMap = Map(
    "jobs" -> Vector(0.15d, 0.85d),
    "harry" -> Vector(0.9d, 0.1d),
    "economy" -> Vector(0.35d, 0.65d)
  )

  "LdaScoringModel" should "throw an IllegalArgumentException if number of topics is less than one" in {
    intercept[IllegalArgumentException] {
      new LdaScoreModel(LdaModel(0, topicWordMap, "doc", "word"))
    }
  }

  "predict" should "compute topic probabilities for document" in {
    val ldaModel = LdaModel(numTopics, topicWordMap, "doc", "word")
    val scoringModel = new LdaScoreModel(ldaModel)

    val documents = Seq(
      Array(List("jobs", "harry", "jobs", "harry", "harry", "new_word")),
      Array(List("jobs", "economy", "economy", "harry")),
      Array(List.empty[String])
    )

    var scores = List[Any]()
    documents.foreach { document =>

      scores = scores :+ scoringModel.score(document.asInstanceOf[Array[Any]])
    }

    scores.length should equal(3)
    val score0 = scores(0).asInstanceOf[Array[Any]]
    val score1 = scores(1).asInstanceOf[Array[Any]]
    val score2 = scores(2).asInstanceOf[Array[Any]]

    val score01 = score0(1).asInstanceOf[Map[String, Any]]
    score01("topics_given_doc").asInstanceOf[List[Double]].toArray should equalWithTolerance(Array(0.5, 0.333333))
    score01("new_words_count").asInstanceOf[Int] should equal(1)
    score01("new_words_percentage").asInstanceOf[Double] should equal(100 / 6d +- epsilon)

    val score11 = score1(1).asInstanceOf[Map[String, Any]]
    score11("topics_given_doc").asInstanceOf[List[Double]].toArray should equalWithTolerance(Array(0.4375, 0.5625))
    score11("new_words_count").asInstanceOf[Int] should equal(0)
    score11("new_words_percentage").asInstanceOf[Double] should equal(0d +- epsilon)

    val score21 = score2(1).asInstanceOf[Map[String, Any]]
    score21("topics_given_doc").asInstanceOf[List[Double]].toArray should equalWithTolerance(Array(0d, 0d))
    score21("new_words_count").asInstanceOf[Int] should equal(0)
    score21("new_words_percentage").asInstanceOf[Double] should equal(0d +- epsilon)
  }

}
