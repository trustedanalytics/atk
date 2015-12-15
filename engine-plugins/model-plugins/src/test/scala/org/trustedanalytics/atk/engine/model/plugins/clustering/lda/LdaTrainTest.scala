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

package org.trustedanalytics.atk.engine.model.plugins.clustering.lda

import org.apache.spark.frame.FrameRdd
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
  val epsilon = 1e-6

  /** assertion that two doubles are almost equal */
  def assertAlmostEqual(x: Double, y: Double, tolerance: Double = 1e-6): Unit = {
    assert(Math.abs(x - y) < tolerance, s"${x} should equal ${y}+-${tolerance}")
  }

  /** assertion that most likely topic is given by index */
  def assertLikelyTopic(v: Vector[Double], topicIndex: Int): Unit = {
    assert(v.indexOf(v.max) == topicIndex, s"topic should equal ${topicIndex}")
  }

  /* assert each element in vector is between 0 and 1 */
  def assertHasValidProbabilities(map: Map[String, Vector[Double]]): Unit = {
    map.foreach {
      case (s, vector) => {
        for (x <- vector) {
          assert(x >= 0 && x <= 1, s"topic probabilities for ${s} should lie between 0 and 1")
        }
      }
    }
  }

  /* assert sum of probabilities in vectors is one */
  def assertProbabilitySumIsOne(map: Map[String, Vector[Double]]): Unit = {
    map.foreach {
      case (s, vector) => {
        assert(Math.round(vector.sum) == 1, s"sum of topic probabilities for ${s} should equal 1")
      }
    }
  }

  "LDA train" should {

    "initialize LDA runner" in {
      val trainArgs = LdaTrainArgs(model, frame, "document", "word", "word_count",
        numTopics = 2, maxIterations = 10, alpha = 1.3f, beta = 1.6f, randomSeed = Some(25))
      val ldaRunner = LdaTrainFunctions.initializeLdaRunner(trainArgs)

      assert(ldaRunner.getK == 2)
      assert(ldaRunner.getMaxIterations == 10)
      assertAlmostEqual(ldaRunner.getAlpha, 1.3)
      assertAlmostEqual(ldaRunner.getBeta, 1.6d)
      assert(ldaRunner.getSeed == 25)
    }

    "compute topic probabilities" in {
      val rows = sparkContext.parallelize(edgeData)
      val edgeFrame = new FrameRdd(edgeSchema, rows)
      val trainArgs = LdaTrainArgs(model, frame, "document", "word", "word_count",
        numTopics = 2, maxIterations = 10, randomSeed = Some(25))
      val ldaModel = LdaTrainFunctions.trainLdaModel(edgeFrame, trainArgs)

      val topicsGivenDoc = ldaModel.getTopicsGivenDocFrame.map(row => {
        (row(0).asInstanceOf[String], row(1).asInstanceOf[Vector[Double]])
      }).collectAsMap()
      val wordGivenTopic = ldaModel.getWordGivenTopicsFrame.map(row => {
        (row(0).asInstanceOf[String], row(1).asInstanceOf[Vector[Double]])
      }).collectAsMap()
      val topicsGivenWord = ldaModel.getTopicsGivenWordFrame.map(row => {
        (row(0).asInstanceOf[String], row(1).asInstanceOf[Vector[Double]])
      }).collectAsMap()

      val harryPotterVector = topicsGivenDoc("harrypotter")
      val harryPotterTopic = harryPotterVector.indexOf(harryPotterVector.max)
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
