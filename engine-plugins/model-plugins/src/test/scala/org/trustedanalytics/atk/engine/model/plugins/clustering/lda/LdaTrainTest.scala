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

import breeze.linalg.DenseVector
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class LdaTrainTest extends TestingSparkContextWordSpec with Matchers {

  val documents = Map[Int, String](
    0 -> "nytimes",
    1 -> "economist",
    2 -> "harrypotter")

  val words = Map[Int, String](
    0 -> "harry",
    1 -> "economy",
    2 -> "jobs",
    3 -> "magic",
    4 -> "realestate",
    5 -> "movies",
    6 -> "chamber",
    7 -> "secrets"
  )

  val ldaData = List[(Long, Vector)](
    (0, new SparseVector(8, Array(0, 1, 2, 3, 4, 5), Array(3, 35, 40, 1, 15, 6))),
    (1, new SparseVector(8, Array(1, 2, 4, 5, 0, 3), Array(50, 35, 20, 1, 1, 1))),
    (2, new SparseVector(8, Array(0, 3, 6, 7), Array(40, 30, 20, 30)))
  )


  "LDA train" should {
    "compute topic probabilities" in {
      val corpus = sparkContext.parallelize(ldaData)
      val ldaModel = new LDA().setK(2).setAlpha(1.2).run(corpus)

      val topicDistributions = ldaModel.topicDistributions.collect()
      val topicMatrix = ldaModel.topicsMatrix
      println(topicDistributions)
      val globalTopicField = classOf[DistributedLDAModel].getDeclaredField("globalTopicTotals")
      globalTopicField.setAccessible(true)
      val globalTopicTotals = globalTopicField.get(ldaModel).asInstanceOf[DenseVector[Double]]
      println(globalTopicTotals)
      /*
      assertVectorApproximate(docResults("nytimes"), 0.9791622779172616, 0.020837722082738406)
      assertVectorApproximate(docResults("economist"), 0.9940318217299755, 0.005968178270024487)
      assertVectorApproximate(docResults("harrypotter"), 8.452603312170585E-4, 0.999154739668783)

      assertVectorApproximate(wordResults("economy"), 0.4139199864115501, 8.281218109226838E-4)
      assertVectorApproximate(wordResults("movies"), 0.03451218015974311, 8.441422016054433E-4)
      assertVectorApproximate(wordResults("jobs"), 0.36527729311947715, 8.312812262932968E-4)
      assertVectorApproximate(wordResults("harry"), 0.012227142764471467, 0.33614890265523867)
      assertVectorApproximate(wordResults("chamber"), 4.8666699891845546E-4, 0.16208167430495116)
      assertVectorApproximate(wordResults("secrets"), 4.866629945216566E-4, 0.242719543561563)
       */
    }
  }
}
