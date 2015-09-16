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

import org.trustedanalytics.atk.engine.frame.plugins.ClassificationMetrics
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class ClassificationMetricTest extends TestingSparkContextFlatSpec with Matchers {

  // posLabel = 1
  // tp = 1
  // tn = 2
  // fp = 0
  // fn = 1
  val inputListBinary = List(
    Row(0, 0),
    Row(1, 1),
    Row(0, 0),
    Row(1, 0))

  val inputListBinaryChar = List(
    Row("no", "no"),
    Row("yes", "yes"),
    Row("no", "no"),
    Row("yes", "no"))

  val inputListBinary2 = List(
    Row(0, 0),
    Row(1, 1),
    Row(0, 1),
    Row(1, 1),
    Row(0, 0),
    Row(0, 1),
    Row(0, 0),
    Row(1, 1),
    Row(1, 1),
    Row(0, 0),
    Row(1, 0),
    Row(0, 1),
    Row(1, 1),
    Row(0, 1))

  // tp + tn = 2
  val inputListMulti = List(
    Row(0, 0),
    Row(1, 2),
    Row(2, 1),
    Row(0, 0),
    Row(1, 0),
    Row(2, 1))

  val inputListMultiChar = List(
    Row("red", "red"),
    Row("green", "blue"),
    Row("blue", "green"),
    Row("red", "red"),
    Row("green", "red"),
    Row("blue", "green"))

  "accuracy measure" should "compute correct value for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.modelAccuracy(rdd, 0, 1)
    metricValue shouldEqual 0.75
  }

  "accuracy measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val metricValue = ClassificationMetrics.modelAccuracy(rdd, 0, 1)
    metricValue shouldEqual 0.75
  }

  "accuracy measure" should "compute correct value for binary classifier 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val metricValue = ClassificationMetrics.modelAccuracy(rdd, 0, 1)
    val diff = (metricValue - 0.6428571).abs
    diff should be <= 0.0000001
  }

  "accuracy measure" should "compute correct value for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.modelAccuracy(rdd, 0, 1)
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "accuracy measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val metricValue = ClassificationMetrics.modelAccuracy(rdd, 0, 1)
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    metricValue.precision shouldEqual 1.0
  }

  "precision measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yes", 1)
    metricValue.precision shouldEqual 1.0
  }

  "precision measure" should "compute correct value for binary classifier 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    val diff = (metricValue.precision - 0.5555555).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yoyoyo", 1)
    metricValue.precision shouldEqual 0.0
  }

  "precision measure" should "compute correct value for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.multiclassModelPrecision(rdd, 0, 1)
    val diff = (metricValue - 0.2222222).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val metricValue = ClassificationMetrics.multiclassModelPrecision(rdd, 0, 1)
    val diff = (metricValue - 0.2222222).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "compute correct value for binary classifier" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    metricValue.recall shouldEqual 0.5
  }

  "recall measure" should "compute correct value for binary classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yes", 1)
    metricValue.recall shouldEqual 0.5
  }

  "recall measure" should "compute correct value for binary classifier 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    val diff = (metricValue.recall - 0.8333333).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yoyoyo", 1)
    metricValue.recall shouldEqual 0.0
  }

  "recall measure" should "compute correct value for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.multiclassModelRecall(rdd, 0, 1)
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val metricValue = ClassificationMetrics.multiclassModelRecall(rdd, 0, 1)
    val diff = (metricValue - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 0.5" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 0.5)
    val diff = (metricValue.fMeasure - 0.8333333).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 0.5" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 0.5)
    val diff = (metricValue.fMeasure - 0.5952380).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 1" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    val diff = (metricValue.fMeasure - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 1 with string labels" in {
    val rdd = sparkContext.parallelize(inputListBinaryChar)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yes", 1)
    val diff = (metricValue.fMeasure - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 1" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 1)
    val diff = (metricValue.fMeasure - 0.6666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier for beta = 2" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 2)
    val diff = (metricValue.fMeasure - 0.5555555).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for binary classifier 2 for beta = 2" in {
    val rdd = sparkContext.parallelize(inputListBinary2)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "1", 2)
    val diff = (metricValue.fMeasure - 0.7575757).abs
    diff should be <= 0.0000001
  }

  "f measure" should "return 0 for binary classifier if posLabel does not exist in label column" in {
    val rdd = sparkContext.parallelize(inputListBinary)

    val metricValue = ClassificationMetrics.binaryClassificationMetrics(rdd, 0, 1, "yoyoyo", 1)
    metricValue.fMeasure shouldEqual 0.0
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 0.5" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.multiclassModelFMeasure(rdd, 0, 1, 0.5)
    val diff = (metricValue - 0.2380952).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 1" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.multiclassModelFMeasure(rdd, 0, 1, 1)
    val diff = (metricValue - 0.2666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 1 with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val metricValue = ClassificationMetrics.multiclassModelFMeasure(rdd, 0, 1, 1)
    val diff = (metricValue - 0.2666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 2" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val metricValue = ClassificationMetrics.multiclassModelFMeasure(rdd, 0, 1, 2)
    val diff = (metricValue - 0.3030303).abs
    diff should be <= 0.0000001
  }

}
