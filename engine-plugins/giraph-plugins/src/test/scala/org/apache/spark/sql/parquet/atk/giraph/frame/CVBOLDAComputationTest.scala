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

import org.trustedanalytics.atk.giraph.algorithms.lda.CVB0LDAComputation
import org.trustedanalytics.atk.giraph.algorithms.lda.CVB0LDAComputation.{ CVB0LDAAggregatorWriter, CVB0LDAMasterCompute }
import org.trustedanalytics.atk.giraph.config.lda._
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.FrameSchema
import org.apache.mahout.math.Vector
import org.scalatest.WordSpec
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

  val edgeData = List(
    "nytimes,harry,3",
    "nytimes,economy,35",
    "nytimes,jobs,40",
    "nytimes,magic,1",
    "nytimes,realestate,15",
    "nytimes,movies,6",
    "economist,economy,50",
    "economist,jobs,35",
    "economist,realestate,20",
    "economist,movies,1",
    "economist,harry,1",
    "economist,magic,1",
    "harrypotter,harry,40",
    "harrypotter,magic,30",
    "harrypotter,chamber,20",
    "harrypotter,secrets,30"
  ).toArray[String]

  "LDA" should {
    "produce results" in {
      val conf = new LdaConfiguration()
      conf.setComputationClass(classOf[CVB0LDAComputation])
      conf.setMasterComputeClass(classOf[CVB0LDAMasterCompute])
      conf.setAggregatorWriterClass(classOf[CVB0LDAAggregatorWriter])
      conf.setEdgeInputFormatClass(classOf[TestingLdaEdgeInputFormat])
      conf.setVertexOutputFormatClass(classOf[TestingLdaVertexOutputFormat])

      val ldaInputConfig = new LdaInputFormatConfig("dummy-input-location", new FrameSchema())
      val ldaOutputConfig = new LdaOutputFormatConfig("dummy-doc-results", "dummy-word-results", "dummy-topic-results")

      val numTopics = 2
      val ldaArgs = new LdaTrainArgs(new ModelReference(1), new FrameReference(2), "dummy_doc", "dummy_word", "dummy_word_count",
        maxIterations = Some(10), numTopics = Some(numTopics))

      val ldaConfig = new LdaConfig(ldaInputConfig, ldaOutputConfig, ldaArgs)
      conf.setLdaConfig(ldaConfig)

      // run internally
      SynchronizedInternalVertexRunner.run(conf, new Array[String](0), edgeData).toList

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
      assertVectorApproximate(docResults("nytimes"), 0.9791622779172616, 0.020837722082738406)
      assertVectorApproximate(docResults("economist"), 0.9940318217299755, 0.005968178270024487)
      assertVectorApproximate(docResults("harrypotter"), 8.452603312170585E-4, 0.999154739668783)

      assertVectorApproximate(wordResults("economy"), 0.4139199864115501, 8.281218109226838E-4)
      assertVectorApproximate(wordResults("movies"), 0.03451218015974311, 8.441422016054433E-4)
      assertVectorApproximate(wordResults("jobs"), 0.36527729311947715, 8.312812262932968E-4)
      assertVectorApproximate(wordResults("harry"), 0.012227142764471467, 0.33614890265523867)
      assertVectorApproximate(wordResults("chamber"), 4.8666699891845546E-4, 0.16208167430495116)
      assertVectorApproximate(wordResults("secrets"), 4.866629945216566E-4, 0.242719543561563)
      assertVectorApproximate(wordResults("magic"), 0.0056778096874241365, 0.2502411087589012)
      assertVectorApproximate(wordResults("realestate"), 0.17071558553290928, 8.288897492542531E-4)


    }
  }

}
