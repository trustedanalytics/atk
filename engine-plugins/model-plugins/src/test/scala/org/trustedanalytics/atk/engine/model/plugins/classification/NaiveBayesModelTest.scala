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

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class NaiveBayesModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "NaiveBayesModel" should "create a NaiveBayesModel" in {
       val modelRef = mock[ModelReference]
       val frameRef = mock[FrameReference]
    val trainArgs = NaiveBayesTrainArgs(modelRef, frameRef, "label",List("obs1","obs2"),Some(0.5))

       val labeledPoint: Array[LabeledPoint] =  Array(new LabeledPoint(1, new DenseVector(Array(16.8973559126,2.6933495054))),
      new LabeledPoint(1, new DenseVector(Array(5.5548729596,2.7777687995))),
      new LabeledPoint(0, new DenseVector(Array(46.1810010826,3.1611961917))),
      new LabeledPoint(0, new DenseVector(Array(44.3117586448,3.3458963222))))

      val rdd = sparkContext.parallelize(labeledPoint)

      val model = new NaiveBayes()
      model.setLambda(trainArgs.lambdaParameter.getOrElse(1.0))
      val naiveBayesModel = model.run(rdd)

    val naiveBayesData = new NaiveBayesData(naiveBayesModel, trainArgs.observationColumns)
    naiveBayesData shouldBe a[NaiveBayesData]
  }

  "NaiveBayesModel" should "thow an IllegalArgumentException for empty observationColumns" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      val trainArgs = NaiveBayesTrainArgs(modelRef, frameRef, "label",List(),Some(0.5))
    }
  }


  "NaiveBayesModel" should "thow an IllegalArgumentException for empty labelColumn" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      val trainArgs = NaiveBayesTrainArgs(modelRef, frameRef, "",List("obs1","obs2"),Some(0.5))
    }
  }

  }
