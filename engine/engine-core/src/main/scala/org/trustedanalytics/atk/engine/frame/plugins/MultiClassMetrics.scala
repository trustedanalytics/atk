/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.frame.{ ConfusionMatrix, ConfusionMatrixEntry }

import scala.reflect.ClassTag

/**
 * Model Accuracy, Precision, Recall, FMeasure, Confusion matrix for multi-class
 *
 * TODO: this class doesn't really belong in the Engine but it is shared code that both frame-plugins and model-plugins need access to
 */
class MultiClassMetrics[T: ClassTag](labelPredictRdd: RDD[ScoreAndLabel[T]],
                                     beta: Double = 1) extends Serializable {

  def this(frameRdd: FrameRdd,
           labelColumn: String,
           predictColumn: String,
           beta: Double = 1,
           frequencyColumn: Option[String] = None) {
    this(frameRdd.toScoreAndLabelRdd[T](labelColumn, predictColumn, frequencyColumn), beta)
  }

  labelPredictRdd.cache()

  lazy val countsByLabel = labelPredictRdd.map(scoreAndLabel => {
    (scoreAndLabel.label, scoreAndLabel.frequency)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val countsByScore = labelPredictRdd.map(scoreAndLabel => {
    (scoreAndLabel.score, scoreAndLabel.frequency)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val truePositivesByLabel = labelPredictRdd.map(scoreAndLabel => {
    val truePositives = if (scoreAndLabel.label.equals(scoreAndLabel.score)) {
      scoreAndLabel.frequency
    }
    else {
      0L
    }
    (scoreAndLabel.label, truePositives)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val falsePositivesByScore = labelPredictRdd.map(scoreAndLabel => {
    val falsePositives = if (!scoreAndLabel.label.equals(scoreAndLabel.score)) {
      scoreAndLabel.frequency
    }
    else {
      0L
    }
    (scoreAndLabel.score, falsePositives)
  }).reduceByKey(_ + _).collectAsMap()

  lazy val totalCount = countsByLabel.map { case (label, count) => count }.sum

  lazy val totalTruePositives = truePositivesByLabel.map { case (label, count) => count }.sum

  /**
   * Compute precision for label
   *
   * Precision = true positives/(true positives + false positives)
   *
   * @param label Class label
   * @return precision
   */
  def precision(label: T): Double = {
    val truePos = truePositivesByLabel.getOrElse(label, 0L)
    val falsePos = falsePositivesByScore.getOrElse(label, 0L)

    (truePos + falsePos) match {
      case 0 => 0d
      case totalPos => truePos / totalPos.toDouble
    }
  }

  /**
   * Compute recall for label
   *
   * Recall = true positives/(true positives + false negatives)
   * @param label Class label
   * @return recall
   */
  def recall(label: T): Double = {
    val truePos = truePositivesByLabel.getOrElse(label, 0L)
    val labelCount = countsByLabel.getOrElse(label, 0L)

    labelCount match {
      case 0 => 0d
      case _ => truePos / labelCount.toDouble
    }
  }

  /**
   * Compute f-measure for label
   *
   * Fmeasure is weighted average of precision and recall
   * @param label Class label
   * @return F-measure
   */
  def fmeasure(label: T): Double = {
    val labelPrecision = precision(label)
    val labelRecall = recall(label)

    (math.pow(beta, 2) * labelPrecision) + labelRecall match {
      case 0 => 0
      case _ => (1 + math.pow(beta, 2)) * ((labelPrecision * labelRecall) / ((math.pow(beta, 2) * labelPrecision) + labelRecall))
    }
  }

  /**
   * Compute precision weighted by label occurrence
   */
  def weightedPrecision(): Double = {
    if (totalCount > 0) {
      countsByLabel.map {
        case (label, labelCount) =>
          labelCount * precision(label)
      }.sum / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute recall weighted by label occurrence
   */
  def weightedRecall(): Double = {
    if (totalCount > 0) {
      countsByLabel.map {
        case (label, labelCount) =>
          labelCount * recall(label)
      }.sum / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute f-measure weighted by label occurrence
   */
  def weightedFmeasure(): Double = {
    if (totalCount > 0) {
      countsByLabel.map {
        case (label, labelCount) =>
          labelCount * fmeasure(label)
      }.sum / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute model accuracy
   */
  def accuracy(): Double = {
    if (totalCount > 0) {
      totalTruePositives / totalCount.toDouble
    }
    else 0d
  }

  /**
   * Compute confusion matrix for labels
   */
  def confusionMatrix(): ConfusionMatrix = {
    val predictionSummary = labelPredictRdd.map(scoreAndLabel => {
      ((scoreAndLabel.score, scoreAndLabel.label), scoreAndLabel.frequency)
    }).reduceByKey(_ + _).collectAsMap()

    val rowLabels = predictionSummary.map { case ((score, label), frequency) => label.toString }.toSet.toList.sorted
    val colLabels = predictionSummary.map { case ((score, label), frequency) => score.toString }.toSet.toList.sorted
    val matrix = ConfusionMatrix(rowLabels, colLabels)
    predictionSummary.foreach {
      case ((score, label), frequency) =>
        matrix.set(score.toString, label.toString, frequency)
    }
    matrix
  }
}
