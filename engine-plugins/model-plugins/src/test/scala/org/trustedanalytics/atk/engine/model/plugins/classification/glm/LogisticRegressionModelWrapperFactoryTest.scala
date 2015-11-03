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


package org.trustedanalytics.atk.engine.model.plugins.classification.glm

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }

class LogisticRegressionModelWrapperFactoryTest extends FlatSpec with Matchers with MockitoSugar {

  "createModel" should "create L-BFGS logistic regression model" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val trainArgs = LogisticRegressionTrainArgs(modelRef, frameRef, "label", List("obs1", "obs2"), optimizer = "LBFGS")

    val model = LogisticRegressionModelWrapperFactory.createModel(trainArgs)

    model shouldBe a[LogisticRegressionModelWrapperWithLBFGS]
  }

  "createModel" should "create SGD logistic regression model" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val trainArgs = LogisticRegressionTrainArgs(modelRef, frameRef, "label", List("obs1", "obs2"), optimizer = "SGD")

    val model = LogisticRegressionModelWrapperFactory.createModel(trainArgs)

    model shouldBe a[LogisticRegressionModelWrapperWithSGD]
  }

  "createModel" should "throw an IllegalArgumentException for unsupported optimizers" in {
    intercept[IllegalArgumentException] {
      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]
      val trainArgs = LogisticRegressionTrainArgs(modelRef, frameRef, "label", List("obs1", "obs2"), optimizer = "INVALID")

      LogisticRegressionModelWrapperFactory.createModel(trainArgs)
    }
  }

}
