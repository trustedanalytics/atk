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

import org.scalatest.Matchers
import org.trustedanalytics.atk.engine.frame.plugins.{MultiClassMetrics, BinaryClassMetrics, ScoreAndLabel}
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class BinaryClassMetricTest extends TestingSparkContextFlatSpec with Matchers {
  // posLabel = 1
  // tp = 1
  // tn = 2
  // fp = 0
  // fn = 1
  val inputListBinary = List(
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 0))

  val inputListBinaryChar = List(
    ScoreAndLabel("no", "no"),
    ScoreAndLabel("yes", "yes"),
    ScoreAndLabel("no", "no"),
    ScoreAndLabel("yes", "no"))

  val inputListBinary2 = List(
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 1),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(0, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 0),
    ScoreAndLabel(0, 1),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 1))

  // tp + tn = 2
  val inputListMulti = List(
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 2),
    ScoreAndLabel(2, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 0),
    ScoreAndLabel(2, 1))

  val inputListMultiChar = List(
    ScoreAndLabel("red", "red"),
    ScoreAndLabel("green", "blue"),
    ScoreAndLabel("blue", "green"),
    ScoreAndLabel("red", "red"),
    ScoreAndLabel("green", "red"),
    ScoreAndLabel("blue", "green"))

  "accuracy measure" should "compute correct value for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val accuracy = binaryClassMetrics.accuracy()
    accuracy shouldEqual 0.75
  }

  "accuracy measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yes")
    val accuracy = binaryClassMetrics.accuracy()
    accuracy shouldEqual 0.75
  }

  "accuracy measure" should "compute correct value for binary classifier 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val accuracy = binaryClassMetrics.accuracy()
    val diff = (accuracy - 0.6428571).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val precision = binaryClassMetrics.precision()
    precision shouldEqual 1.0
  }

  "precision measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yes")
    val precision = binaryClassMetrics.precision()
    precision shouldEqual 1.0
  }

  "precision measure" should "compute correct value for binary classifier 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val precision = binaryClassMetrics.precision()
    val diff = (precision - 0.5555555).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yoyoyo")
    val precision = binaryClassMetrics.precision()
    precision shouldEqual 0.0
  }

  "recall measure" should "compute correct value for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val recall = binaryClassMetrics.recall()
    recall shouldEqual 0.5
  }

  "recall measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yes")
    val recall = binaryClassMetrics.recall()
    recall shouldEqual 0.5
  }

  "recall measure" should "compute correct value for binary classifier 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1)
    val recall = binaryClassMetrics.recall()
    val diff = (recall - 0.8333333).abs
    diff should be <= 0.0000001
  }

   "recall measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
     val rdd = sparkContext.parallelize(inputListBinary)

     val binaryClassMetrics = new BinaryClassMetrics(rdd, "yoyoyo")
     val recall = binaryClassMetrics.recall()
     recall shouldEqual 0.0
   }

  "f measure" should "compute correct value for binary classifier for beta = 0.5" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1, 0.5)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.8333333).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 0.5" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1, 0.5)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.5952380).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 1" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1, 1)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 1 with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yes", 1)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 1" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1, 1)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 2" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1, 2)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.5555555).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, 1, 2)
    val fmeasure = binaryClassMetrics.fmeasure()
    val diff = (fmeasure - 0.7575757).abs
    diff should be <= 0.0000001
  }

  "f measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val binaryClassMetrics = new BinaryClassMetrics(rdd, "yoyoyo", 1)
    val fmeasure = binaryClassMetrics.fmeasure()
    fmeasure shouldEqual 0.0
  }

}
