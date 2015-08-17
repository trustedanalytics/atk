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

package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.domain.frame.ClassificationMetricValue
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

//implicit conversion for PairRDD

/**
 * Model Accuracy, Precision, Recall, FMeasure, ConfusionMatrix
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 *
 * TODO: this class doesn't really belong in the Engine but it is shared code that both frame-plugins and graph-plugins need access to
 */
object ClassificationMetrics extends Serializable {

  /**
   * Compute accuracy of a classification model
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @return a Double of the model accuracy measure
   */
  def modelAccuracy(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val k = frameRdd.count()
    val t = frameRdd.sparkContext.accumulator[Long](0)

    frameRdd.foreach(row =>
      if (row(labelColumnIndex).toString.equals(row(predColumnIndex).toString)) {
        t.add(1)
      }
    )

    k match {
      case 0 => 0
      case _ => t.value / k.toDouble
    }
  }

  /**
   * compute precision for multi-class classifier using weighted averaging
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @return a Double of the model precision measure
   */
  def multiclassModelPrecision(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

    val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)
    val predictGroupedRdd = pairedRdd.groupBy(pair => pair._2)

    val joinedRdd = labelGroupedRdd.join(predictGroupedRdd)

    val weightedPrecisionRdd: RDD[Double] = joinedRdd.map { label =>
      // label is tuple of (labelValue, (SeqOfInstancesWithThisActualLabel, SeqOfInstancesWithThisPredictedLabel))
      val labelCount = label._2._1.size // get the number of instances with this label as the actual label
      var correctPredict: Long = 0
      val totalPredict = label._2._2.size

      label._2._1.foreach { prediction =>
        if (prediction._1.equals(prediction._2)) {
          correctPredict += 1
        }
      }
      totalPredict match {
        case 0 => 0
        case _ => labelCount * (correctPredict / totalPredict.toDouble)
      }
    }
    weightedPrecisionRdd.sum() / pairedRdd.count().toDouble
  }

  /**
   * compute recall for multi-class classifier using weighted averaging
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @return a Double of the model recall measure
   */
  def multiclassModelRecall(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

    val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)

    val weightedRecallRdd: RDD[Double] = labelGroupedRdd.map { label =>
      // label is tuple of (labelValue, SeqOfInstancesWithThisActualLabel)
      var correctPredict: Long = 0

      label._2.foreach { prediction =>
        if (prediction._1.equals(prediction._2)) {
          correctPredict += 1
        }
      }
      correctPredict
    }
    weightedRecallRdd.sum() / pairedRdd.count().toDouble

  }

  /**
   * compute f measure for multi-class classifier using weighted averaging
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param beta the beta value to use to compute the f measure
   * @return a Double of the model f measure
   */
  def multiclassModelFMeasure(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, beta: Double): Double = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val pairedRdd = frameRdd.map(row => (row(labelColumnIndex).toString, row(predColumnIndex).toString)).cache()

    val labelGroupedRdd = pairedRdd.groupBy(pair => pair._1)
    val predictGroupedRdd = pairedRdd.groupBy(pair => pair._2)

    val joinedRdd = labelGroupedRdd.join(predictGroupedRdd)

    val weightedFMeasureRdd: RDD[Double] = joinedRdd.map { label =>
      // label is tuple of (labelValue, (SeqOfInstancesWithThisActualLabel, SeqOfInstancesWithThisPredictedLabel))
      val labelCount = label._2._1.size // get the number of instances with this label as the actual label
      var correctPredict: Long = 0
      val totalPredict = label._2._2.size

      label._2._1.foreach { prediction =>
        if (prediction._1.equals(prediction._2)) {
          correctPredict += 1
        }
      }

      val precision = totalPredict match {
        case 0 => 0
        case _ => labelCount * (correctPredict / totalPredict.toDouble)
      }

      val recall = labelCount match {
        case 0 => 0
        case _ => labelCount * (correctPredict / labelCount.toDouble)
      }

      (math.pow(beta, 2) * precision) + recall match {
        case 0 => 0
        case _ => (1 + math.pow(beta, 2)) * ((precision * recall) / ((math.pow(beta, 2) * precision) + recall))
      }
    }
    weightedFMeasureRdd.sum() / pairedRdd.count().toDouble
  }

  /**
   * compute classification metrics for multi-class classifier using weighted averaging
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param beta the beta value to use to compute the f measure
   * @return a Double of the model f measure, a Double of the model accuracy, a Double of the model recall, a Double of the model precision, a map of confusion matrix values
   */
  def multiclassClassificationMetrics(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, beta: Double): ClassificationMetricValue = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val precision = multiclassModelPrecision(frameRdd, labelColumnIndex, predColumnIndex)
    val fmeasure: Double = multiclassModelFMeasure(frameRdd, labelColumnIndex, predColumnIndex, beta)
    val recall = multiclassModelRecall(frameRdd, labelColumnIndex, predColumnIndex)
    val accuracy = modelAccuracy(frameRdd, labelColumnIndex, predColumnIndex)

    //TODO: Confusion matrix for multi class classifiers is not yet supported
    ClassificationMetricValue(fmeasure, accuracy, recall, precision, Map())
  }

  /**
   * compute classification metrics for binary classifier
   *
   * @param frameRdd the dataframe RDD containing the labeled and predicted columns
   * @param labelColumnIndex column index for the correctly labeled data
   * @param predColumnIndex column index for the model prediction
   * @param beta the beta value to use to compute the f measure
   * @param posLabel posLabel the label for a positive instance
   * @return a Double of the model f measure, a Double of the model accuracy, a Double of the model recall, a Double of the model precision, a map of confusion matrix values
   */
  def binaryClassificationMetrics(frameRdd: RDD[Row], labelColumnIndex: Int, predColumnIndex: Int, posLabel: String, beta: Double): ClassificationMetricValue = {
    require(labelColumnIndex >= 0, "label column index must be greater than or equal to zero")
    require(predColumnIndex >= 0, "prediction column index must be greater than or equal to zero")

    val k = frameRdd.count()
    val t = frameRdd.sparkContext.accumulator[Long](0)
    val tp = frameRdd.sparkContext.accumulator[Long](0)
    val tn = frameRdd.sparkContext.accumulator[Long](0)
    val fp = frameRdd.sparkContext.accumulator[Long](0)
    val fn = frameRdd.sparkContext.accumulator[Long](0)

    frameRdd.foreach { row =>
      if (row(labelColumnIndex) == row(predColumnIndex)) {
        t.add(1)
      }
      if (row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
        tp.add(1)
      }
      else if (!row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
        tn.add(1)
      }
      else if (!row(labelColumnIndex).toString.equals(posLabel) && row(predColumnIndex).toString.equals(posLabel)) {
        fp.add(1)
      }
      else if (row(labelColumnIndex).toString.equals(posLabel) && !row(predColumnIndex).toString.equals(posLabel)) {
        fn.add(1)
      }
    }

    val precision = tp.value + fp.value match {
      case 0 => 0
      case _ => tp.value / (tp.value + fp.value).toDouble
    }

    val recall = tp.value + fn.value match {
      case 0 => 0
      case _ => tp.value / (tp.value + fn.value).toDouble
    }

    val fmeasure = math.pow(beta, 2) * precision + recall match {
      case 0 => 0
      case _ => (1 + math.pow(beta, 2)) * ((precision * recall) / ((math.pow(beta, 2) * precision) + recall))
    }

    val accuracy = k match {
      case 0 => 0
      case _ => t.value / k.toDouble
    }

    ClassificationMetricValue(fmeasure, accuracy, recall, precision, Map(("tp", tp.value), ("tn", tn.value), ("fp", fp.value), ("fn", fn.value)))
  }
}
