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
package org.trustedanalytics.atk.engine.model.plugins.clustering.lda

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ SparseVector, Vector }
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class LdaTrainTest extends TestingSparkContextWordSpec with Matchers {

  val edgeData: Array[Row] = Array(
    Row("nytimes", "harry", 3L),
    Row("nytimes", "economy", 35L),
    Row("nytimes", "jobs", 40L),
    Row("nytimes", "magic", 1L),
    Row("nytimes", "realestate", 15L),
    Row("nytimes", "movies", 6L),
    Row("economist", "economy", 50L),
    Row("economist", "jobs", 35L),
    Row("economist", "realestate", 20L),
    Row("economist", "movies", 1L),
    Row("economist", "harry", 1L),
    Row("economist", "magic", 1L),
    Row("harrypotter", "harry", 40L),
    Row("harrypotter", "magic", 30L),
    Row("harrypotter", "chamber", 20L),
    Row("harrypotter", "secrets", 30L)
  )

  val edgeSchema = FrameSchema(List(
    Column("document", DataTypes.string),
    Column("word", DataTypes.string),
    Column("word_count", DataTypes.int64)
  ))

  val model = new ModelReference(1)
  val frame = new FrameReference(1)
  val trainArgs = LdaTrainArgs(model, frame, "document", "word", "word_count", numTopics = 2, maxIterations = 10)

  val ldaData = List[(Long, Vector)](
    (0, new SparseVector(8, Array(0, 1, 2, 3, 4, 5), Array(3, 35, 40, 1, 15, 6))),
    (1, new SparseVector(8, Array(1, 2, 4, 5, 0, 3), Array(50, 35, 20, 1, 1, 1))),
    (2, new SparseVector(8, Array(0, 3, 6, 7), Array(40, 30, 20, 30)))
  )

  /** round to so many decimal places */
  def round(d: Double): BigDecimal = {
    BigDecimal(d).setScale(5, BigDecimal.RoundingMode.HALF_UP)
  }

  /** assertion that checks values are close */
  def assertVectorApproximate(v: Vector, expectedDouble1: Double, expectedDouble2: Double): Unit = {
    assert(round(v(0)) == round(expectedDouble1))
    assert(round(v(1)) == round(expectedDouble2))
  }

  /** assertion that most likely topic is given by index */
  def assertLikelyTopic(v: Vector, topicIndex: Int): Unit = {
    val arr = v.toArray
    assert(arr.indexOf(arr.max) == topicIndex, s"topic should equal ${topicIndex}")
  }

  /* assert each element in vector is between 0 and 1 */
  def assertHasValidProbabilities(map: Map[String, Vector]): Unit = {
    map.foreach {
      case (s, vector) => {
        for (x <- vector.toArray) {
          assert(x >= 0 && x <= 1, s"topic probabilities for ${s} should lie between 0 and 1")
        }
      }
    }
  }

  /* assert sum of probabilities in vectors is one */
  def assertProbabilitySumIsOne(map: Map[String, Vector]): Unit = {
    map.foreach {
      case (s, vector) => {
        assert(round(vector.toArray.sum) == 1, s"sum of topic probabilities for ${s} should equal 1")
      }
    }
  }

  "LDA train" should {
    "compute topic probabilities" in {
      val rows = sparkContext.parallelize(edgeData)
      val edgeFrame = new FrameRdd(edgeSchema, rows)
      val ldaModel = LdaFunctions.trainLdaModel(edgeFrame, trainArgs)

      val topicsGivenDoc = ldaModel.getTopicsGivenDocFrame.map(row => {
        (row(0).asInstanceOf[String], row(1).asInstanceOf[Vector])
      }).collectAsMap()
      val wordGivenTopic = ldaModel.getWordGivenTopicsFrame.map(row => {
        (row(0).asInstanceOf[String], row(1).asInstanceOf[Vector])
      }).collectAsMap()
      val topicsGivenWord = ldaModel.getTopicsGivenWordFrame.map(row => {
        (row(0).asInstanceOf[String], row(1).asInstanceOf[Vector])
      }).collectAsMap()

      val harryPotterArr = topicsGivenDoc("harrypotter").toArray
      val harryPotterTopic = harryPotterArr.indexOf(harryPotterArr.max)
      val newsTopic = 1 - harryPotterTopic

      assertLikelyTopic(topicsGivenDoc("nytimes"), newsTopic)
      assertLikelyTopic(topicsGivenDoc("economist"), newsTopic)
      assertLikelyTopic(topicsGivenDoc("harrypotter"), harryPotterTopic)

      assertLikelyTopic(wordGivenTopic("economy"), newsTopic)
      assertLikelyTopic(wordGivenTopic("movies"), newsTopic)
      assertLikelyTopic(wordGivenTopic("jobs"), newsTopic)
      assertLikelyTopic(wordGivenTopic("harry"), harryPotterTopic)
      assertLikelyTopic(wordGivenTopic("chamber"), harryPotterTopic)
      assertLikelyTopic(wordGivenTopic("secrets"), harryPotterTopic)
      assertLikelyTopic(wordGivenTopic("magic"), harryPotterTopic)
      assertLikelyTopic(wordGivenTopic("realestate"), newsTopic)

      assertHasValidProbabilities(topicsGivenDoc.toMap)
      assertHasValidProbabilities(topicsGivenWord.toMap)
      assertProbabilitySumIsOne(topicsGivenDoc.toMap)
      assertProbabilitySumIsOne(topicsGivenWord.toMap)
    }
  }
}
