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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.SerializableType
import org.trustedanalytics.atk.domain.frame.ConfusionMatrixEntry



/**
 * Model Accuracy, Precision, Recall, FMeasure, Confusion matrix for binary-class
 *
 * TODO: this class doesn't really belong in the Engine but it is shared code that both frame-plugins and model-plugins need access to
 */
case class BinaryClassCounter(var truePositives: Long = 0, 
                               var falsePositives: Long = 0, 
                               var trueNegatives: Long = 0, 
                               var falseNegatives: Long = 0, 
                               var count: Long = 0) {
  def +(that: BinaryClassCounter): BinaryClassCounter = {
    this.truePositives += that.truePositives
    this.falsePositives += that.falsePositives
    this.trueNegatives += that.trueNegatives
    this.falseNegatives += that.falseNegatives
    this.count += that.count
    this
  }
}

case class BinaryClassMetrics[T, S : SerializableType](labelPredictRdd: RDD[ScoreAndLabel[T]],
                                         positiveLabel: S,
                                         beta: Double = 1) extends Serializable {

  def this(frameRdd: FrameRdd,
           labelColumn: String,
           predictColumn: String,
           positiveLabel1: S,
           beta: Double = 1,
           frequencyColumn: Option[String] = None) {

    this(frameRdd.toScoreAndLabelRdd[T](labelColumn, predictColumn, frequencyColumn), positiveLabel1, beta)
  }

  lazy val counter: BinaryClassCounter = labelPredictRdd.map(scoreAndLabel => {
    val counter = BinaryClassCounter()
    val score = scoreAndLabel.score
    val label = scoreAndLabel.label
    val frequency = scoreAndLabel.frequency
    counter.count += frequency

    if (label.equals(positiveLabel) && score.equals(positiveLabel)) {
      counter.truePositives += frequency
    }
    else if (!label.equals(positiveLabel) && !score.equals(positiveLabel)) {
      counter.trueNegatives += frequency
    }
    else if (!label.equals(positiveLabel) && score.equals(positiveLabel)) {
      counter.falsePositives += frequency
    }
    else if (label.equals(positiveLabel) && !score.equals(positiveLabel)) {
      counter.falseNegatives += frequency
    }
    counter
  }).reduce((counter1, counter2) => counter1 + counter2)

  lazy val count = counter.count
  lazy val truePositives = counter.truePositives
  lazy val trueNegatives = counter.trueNegatives
  lazy val falsePositives = counter.falsePositives
  lazy val falseNegatives = counter.falseNegatives

  /**
   * Compute precision = true positives/(true positives + false positives)
   */
  def precision(): Double = {
    truePositives + falsePositives match {
      case 0 => 0
      case _ => truePositives / (truePositives + falsePositives).toDouble
    }
  }

  /**
   * Compute recall = true positives/(true positives + false negatives)
   */
  def recall(): Double = {
    truePositives + falseNegatives match {
      case 0 => 0
      case _ => truePositives / (truePositives + falseNegatives).toDouble
    }
  }

  /**
   * Compute f-measure = weighted average of precision and recall
   */
  def fmeasure(): Double = {
    math.pow(beta, 2) * precision + recall match {
      case 0 => 0
      case _ => (1 + math.pow(beta, 2)) * ((precision * recall) / ((math.pow(beta, 2) * precision) + recall))
    }
  }

  /**
   * Compute model accuracy
   */
  def accuracy(): Double = {
    count match {
      case 0 => 0
      case total => (truePositives + trueNegatives) / total.toDouble
    }
  }

  /**
   * Compute confusion matrix
   */
  def confusionMatrix(): List[ConfusionMatrixEntry] = {
    List(ConfusionMatrixEntry("positive", "positive", truePositives),
      ConfusionMatrixEntry("positive", "negative", falseNegatives),
      ConfusionMatrixEntry("negative", "positive", falsePositives),
      ConfusionMatrixEntry("negative", "negative", trueNegatives)
    )
  }

}
