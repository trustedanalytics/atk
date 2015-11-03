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

package org.trustedanalytics.atk.engine.frame.plugins.classificationmetrics

import org.scalatest.Matchers
import org.trustedanalytics.atk.engine.frame.plugins.{ MultiClassMetrics, ScoreAndLabel }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class MultiClassMetricTest extends TestingSparkContextFlatSpec with Matchers {

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

  val inputListBinary2 = List(
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(1, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 0),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(0, 0),
    ScoreAndLabel(0, 1),
    ScoreAndLabel(1, 0),
    ScoreAndLabel(1, 1),
    ScoreAndLabel(1, 0))

  // tp + tn = 2
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

  "accuracy measure" should "compute correct value for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val accuracy = multiClassMetrics.accuracy()
    val diff = (accuracy - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "accuracy measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val accuracy = multiClassMetrics.accuracy()
    val diff = (accuracy - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val precision = multiClassMetrics.weightedPrecision()
    val diff = (precision - 0.2222222).abs
    diff should be <= 0.0000001
  }

  "precision measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val precision = multiClassMetrics.weightedPrecision()
    val diff = (precision - 0.2222222).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "compute correct value for multi-class classifier" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val recall = multiClassMetrics.weightedRecall()
    val diff = (recall - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "recall measure" should "compute correct value for multi-class classifier with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val multiClassMetrics = new MultiClassMetrics(rdd)
    val recall = multiClassMetrics.weightedRecall()
    val diff = (recall - 0.3333333).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 0.5" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd, 0.5)
    val fmeasure = multiClassMetrics.weightedFmeasure()
    val diff = (fmeasure - 0.2380952).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 1" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd, 1)
    val fmeasure = multiClassMetrics.weightedFmeasure()
    val diff = (fmeasure - 0.2666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 1 with string labels" in {
    val rdd = sparkContext.parallelize(inputListMultiChar)

    val multiClassMetrics = new MultiClassMetrics(rdd, 1)
    val fmeasure = multiClassMetrics.weightedFmeasure()
    val diff = (fmeasure - 0.2666666).abs
    diff should be <= 0.0000001
  }

  "f measure" should "compute correct value for multi-class classifier for beta = 2" in {
    val rdd = sparkContext.parallelize(inputListMulti)

    val multiClassMetrics = new MultiClassMetrics(rdd, 2)
    val fmeasure = multiClassMetrics.weightedFmeasure()
    val diff = (fmeasure - 0.3030303).abs
    diff should be <= 0.0000001
  }
}
