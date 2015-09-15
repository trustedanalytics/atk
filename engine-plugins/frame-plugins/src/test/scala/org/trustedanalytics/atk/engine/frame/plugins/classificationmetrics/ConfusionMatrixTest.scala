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

package org.trustedanalytics.atk.engine.frame.plugins.classificationmetrics

import org.trustedanalytics.atk.domain.frame.ConfusionMatrixEntry
import org.trustedanalytics.atk.engine.frame.plugins.{MultiClassMetrics, BinaryClassMetrics, ScoreAndLabel, ClassificationMetrics}
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class ConfusionMatrixTest extends TestingSparkContextFlatSpec with Matchers {

  // posLabel = 1
  // tp = 1
  // tn = 2
  // fp = 0
  // fn = 1
  val inputListBinary = List(
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(0, 1))

  val inputListBinaryChar = List(
    ScoreAndLabel("no", "no"),
    ScoreAndLabel("yes", "yes"),
    ScoreAndLabel("no", "no"),
    ScoreAndLabel("no", "yes"))

  val inputListMulti = List(
    ScoreAndLabel(0, 0),
    ScoreAndLabel(2, 1),
    ScoreAndLabel(1, 2),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(0, 1),
    ScoreAndLabel(1, 2))

  val inputListMultiChar = List(
    ScoreAndLabel("red", "red"),
    ScoreAndLabel("blue", "green"),
    ScoreAndLabel("green", "blue"),
    ScoreAndLabel("red", "red"),
    ScoreAndLabel("red", "green"),
    ScoreAndLabel("green", "blue"))

  "confusion matrix" should "compute predicted vs. actual class for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)
    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val confusionMatrix = binaryClassMetrics.confusionMatrix()

    val expectedConfusionMatrix = List(
      ConfusionMatrixEntry("positive", "positive", 1),
      ConfusionMatrixEntry("positive", "negative", 0),
      ConfusionMatrixEntry("negative", "positive", 1),
      ConfusionMatrixEntry("negative", "negative", 2))

    confusionMatrix should contain theSameElementsAs(expectedConfusionMatrix)
  }

  "confusion matrix" should "compute predicted vs. actual class for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yes")
    val confusionMatrix = binaryClassMetrics.confusionMatrix()

    val expectedConfusionMatrix = List(
      ConfusionMatrixEntry("positive", "positive", 1),
      ConfusionMatrixEntry("positive", "negative", 0),
      ConfusionMatrixEntry("negative", "positive", 1),
      ConfusionMatrixEntry("negative", "negative", 2))

    confusionMatrix should contain theSameElementsAs(expectedConfusionMatrix)
  }

  "confusion matrix" should "compute predicted vs. actual class for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val confusionMatrix = multiClassMetrics.confusionMatrix()

    val expectedConfusionMatrix = List(
      ConfusionMatrixEntry("0", "0", 2),
      ConfusionMatrixEntry("0", "1", 1),
      ConfusionMatrixEntry("1", "2", 2),
      ConfusionMatrixEntry("2", "1", 1))

    confusionMatrix should contain theSameElementsAs(expectedConfusionMatrix)
  }

  "confusion matrix" should "compute predicted vs. actual class for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val confusionMatrix = multiClassMetrics.confusionMatrix()

    val expectedConfusionMatrix = List(
      ConfusionMatrixEntry("red", "red", 2),
      ConfusionMatrixEntry("red", "green", 1),
      ConfusionMatrixEntry("green", "blue", 2),
      ConfusionMatrixEntry("blue", "green", 1))

    confusionMatrix should contain theSameElementsAs(expectedConfusionMatrix)
  }

}
