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
package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.ArgDoc

case class ClassificationMetricArgs(frame: FrameReference,
                                    @ArgDoc("""The name of the column containing the
correct label for each instance.""") labelColumn: String,
                                    @ArgDoc("""The name of the column containing the
predicted label for each instance.""") predColumn: String,
                                    @ArgDoc("""This is a str or int for binary classifiers,
and Null for multi-class classifiers.
The value to be interpreted as a positive instance.""") posLabel: Option[Either[String, Int]],
                                    @ArgDoc("""This is the beta value to use for
:math:`F_{ \beta}` measure (default F1 measure is computed); must be greater than zero.
Defaults is 1.""") beta: Option[Double] = None,
                                    @ArgDoc("""The name of an optional column containing the
frequency of observations.""") frequencyColumn: Option[String] = None) {
  require(frame != null, "ClassificationMetric requires a non-null dataframe.")
  require(labelColumn != null && !labelColumn.equals(""), "label column is required")
  require(predColumn != null && !predColumn.equals(""), "predict column is required")
  beta match {
    case Some(x) => require(x >= 0, "invalid beta value for f measure. Should be greater than or equal to 0")
    case _ => null
  }
}

/**
 * Entry in confusion matrix
 *
 * @param predictedClass Predicted class label
 * @param actualClass Actual class label
 * @param count Count of instances matching actual class and predicted class
 */
case class ConfusionMatrixEntry(predictedClass: String, actualClass: String, count: Long)

case class ConfusionMatrix(rowLabels: List[String], columnLabels: List[String]) {
  val numRows = rowLabels.size
  val numColumns = columnLabels.size
  private var matrix: Array[Array[Long]] = Array.fill(numRows) { Array.fill(numColumns) { 0L } }

  def set(predictedClass: String, actualClass: String, count: Long): Unit = {
    matrix(rowIndex(actualClass))(columnIndex(predictedClass)) = count
  }

  def get(predictedClass: String, actualClass: String): Long = {
    matrix(rowIndex(actualClass))(columnIndex(predictedClass))
  }

  def getMatrix: Array[Array[Long]] = matrix

  def setMatrix(matrix: Array[Array[Long]]): Unit = {
    this.matrix = matrix
  }

  /**
   * get row index by row name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param rowName name of the column to find index
   */
  def rowIndex(rowName: String): Int = {
    val index = rowLabels.indexWhere(row => row == rowName, 0)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid row name $rowName provided, please choose from: " + rowLabels)
    else
      index
  }

  /**
   * get column index by column name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = {
    val index = columnLabels.indexWhere(column => column == columnName, 0)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid column name $columnName provided, please choose from: " + columnLabels)
    else
      index
  }
}

/**
 * Classification metrics
 *
 * @param fMeasure Weighted average of precision and recall
 * @param accuracy Fraction of correct predictions
 * @param recall Fraction of positives correctly predicted
 * @param precision Fraction of correct predictions among positive predictions
 * @param confusionMatrix Matrix of actual vs. predicted classes
 */
case class ClassificationMetricValue(fMeasure: Double,
                                     accuracy: Double,
                                     recall: Double,
                                     precision: Double,
                                     confusionMatrix: ConfusionMatrix)
