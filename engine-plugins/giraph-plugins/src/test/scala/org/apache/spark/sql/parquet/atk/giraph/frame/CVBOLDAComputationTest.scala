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

package org.apache.spark.sql.parquet.atk.giraph.frame

import org.apache.mahout.math.Vector
import org.scalatest.WordSpec
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.FrameSchema
import org.trustedanalytics.atk.giraph.algorithms.lda.CVB0LDAComputation
import org.trustedanalytics.atk.giraph.algorithms.lda.CVB0LDAComputation.{ CVB0LDAAggregatorWriter, CVB0LDAMasterCompute }
import org.trustedanalytics.atk.giraph.config.lda._
import org.trustedanalytics.atk.giraph.testutils.SynchronizedInternalVertexRunner

import scala.collection.JavaConversions._

class CVBOLDAComputationTest extends WordSpec {

  /** round to so many decimal places */
  def round(d: Double): BigDecimal = {
    BigDecimal(d).setScale(5, BigDecimal.RoundingMode.HALF_UP)
  }

  /** assertion that checks values are close */
  def assertVectorApproximate(v: Vector, expectedDouble1: Double, expectedDouble2: Double): Unit = {
    assert(round(v.get(0)) == round(expectedDouble1))
    assert(round(v.get(1)) == round(expectedDouble2))
  }

  /** assertion that most likely topic is given by index */
  def assertLikelyTopic(v: Vector, topicIndex: Int): Unit = {
    assert(v.maxValueIndex() == topicIndex, s"topic should equal ${topicIndex}")
  }

  /* assert each element in vector is between 0 and 1 */
  def assertHasValidProbabilities(map: Map[String, Vector]): Unit = {
    map.foreach {
      case (s, vector) => {
        for (x <- vector.all()) {
          assert(x.get() >= 0 && x.get() <= 1, s"topic probabilities for ${s} should lie between 0 and 1")
        }
      }
    }
  }

  /* assert sum of probabilities in vectors is one */
  def assertProbabilitySumIsOne(map: Map[String, Vector]): Unit = {
    map.foreach {
      case (s, vector) => {
        for (x <- vector.all()) {
          assert(round(vector.zSum()) == 1, s"sum of topic probabilities for ${s} should equal 1")
        }
      }
    }
  }

  val vertexData = List(
    "1,nytimes,1",
    "2,harry,0",
    "3,economy,0",
    "4,jobs,0",
    "5,magic,0",
    "6,realestate,0",
    "7,movies,0",
    "8,economist,1",
    "9,harrypotter,1",
    "10,chamber,0",
    "11,secrets,0"
  ).toArray[String]

  val edgeData = List(
    "nytimes,harry,3,1,2",
    "nytimes,economy,35,1,3",
    "nytimes,jobs,40,1,4",
    "nytimes,magic,1,1,5",
    "nytimes,realestate,15,1,6",
    "nytimes,movies,6,1,7",
    "economist,economy,50,8,3",
    "economist,jobs,35,8,4",
    "economist,realestate,20,8,6",
    "economist,movies,1,8,7",
    "economist,harry,1,8,2",
    "economist,magic,1,8,5",
    "harrypotter,harry,40,9,2",
    "harrypotter,magic,30,9,5",
    "harrypotter,chamber,20,9,10",
    "harrypotter,secrets,30,9,11"
  ).toArray[String]

  "LDA" should {
    "produce results" in {
      val conf = new LdaConfiguration()
      conf.setComputationClass(classOf[CVB0LDAComputation])
      conf.setMasterComputeClass(classOf[CVB0LDAMasterCompute])
      conf.setAggregatorWriterClass(classOf[CVB0LDAAggregatorWriter])
      conf.setEdgeInputFormatClass(classOf[TestingLdaEdgeInputFormat])
      conf.setVertexInputFormatClass(classOf[TestingLdaVertexInputFormat])
      conf.setVertexOutputFormatClass(classOf[TestingLdaVertexOutputFormat])

      val ldaInputConfig = new LdaInputFormatConfig("dummy-edge-input-location", new FrameSchema(), "dummy-vertex-input-location", new FrameSchema())
      val ldaOutputConfig = new LdaOutputFormatConfig("dummy-doc-results", "dummy-word-results", "dummy-topic-results")

      val numTopics = 2
      val ldaArgs = new LdaTrainArgs(new ModelReference(1), new FrameReference(2), "dummy_doc", "dummy_word", "dummy_word_count",
        maxIterations = Some(10), numTopics = Some(numTopics))

      val ldaConfig = new LdaConfig(ldaInputConfig, ldaOutputConfig, ldaArgs, new LdaVertexInputFormatConfig(ldaArgs))
      conf.setLdaConfig(ldaConfig)

      // run internally
      SynchronizedInternalVertexRunner.run(conf, vertexData, edgeData).toList

      // validate results that were stored into a global
      val docResults = TestingLdaOutputResults.docResults
      val wordResults = TestingLdaOutputResults.wordResults
      val topicGivenWord = TestingLdaOutputResults.topicGivenWord

      // validate correct number of results
      assert(docResults.size == 3)
      assert(wordResults.size == 8)
      assert(topicGivenWord.size == 8)

      // each result should have a vector with as many elements as the number of topics
      docResults.foreach { case (s, vector) => assert(vector.size() == numTopics, s"result $s should have a vector of results with size of numTopics") }
      wordResults.foreach { case (s, vector) => assert(vector.size() == numTopics, s"result $s should have a vector of results with size of numTopics") }
      topicGivenWord.foreach { case (s, vector) => assert(vector.size() == numTopics, s"result $s should have a vector of results with size of numTopics") }

      // there is a random element in the results so we round to so many decimal places
      val harryPotterTopic = docResults("harrypotter").maxValueIndex()
      val newsTopic = 1 - harryPotterTopic

      assertLikelyTopic(docResults("nytimes"), newsTopic)
      assertLikelyTopic(docResults("economist"), newsTopic)
      assertLikelyTopic(docResults("harrypotter"), harryPotterTopic)

      assertLikelyTopic(wordResults("economy"), newsTopic)
      assertLikelyTopic(wordResults("movies"), newsTopic)
      assertLikelyTopic(wordResults("jobs"), newsTopic)
      assertLikelyTopic(wordResults("harry"), harryPotterTopic)
      assertLikelyTopic(wordResults("chamber"), harryPotterTopic)
      assertLikelyTopic(wordResults("secrets"), harryPotterTopic)
      assertLikelyTopic(wordResults("magic"), harryPotterTopic)
      assertLikelyTopic(wordResults("realestate"), newsTopic)

      assertHasValidProbabilities(docResults.toMap)
      assertHasValidProbabilities(topicGivenWord.toMap)
      assertProbabilitySumIsOne(docResults.toMap)
      assertProbabilitySumIsOne(topicGivenWord.toMap)

    }
  }

}
