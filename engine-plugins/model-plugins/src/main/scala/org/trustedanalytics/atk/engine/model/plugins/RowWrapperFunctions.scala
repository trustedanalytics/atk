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


package org.trustedanalytics.atk.engine.model.plugins

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.{ LabeledPoint, LabeledPointWithFrequency }
import breeze.linalg.{ DenseVector => BDV }

class RowWrapperFunctions(self: RowWrapper) {

  /**
   * Convert Row into MLlib Dense Vector
   */
  def valuesAsDenseVector(columnNames: List[String]): DenseVector = {
    val array = self.valuesAsDoubleArray(columnNames)
    new DenseVector(array)
  }

  /**
   * Convert Row into MLlib Dense Vector
   */
  def valuesAsBreezeDenseVector(columnNames: List[String]): BDV[Double] = {
    val array = self.valuesAsDoubleArray(columnNames)
    new BDV[Double](array)
  }

  /**
   * Convert Row into LabeledPoint format required by MLLib
   */
  def valuesAsLabeledPoint(featureColumnNames: List[String], labelColumnName: String): LabeledPoint = {
    val label = DataTypes.toDouble(self.value(labelColumnName))
    val vector = valuesAsDenseVector(featureColumnNames)
    new LabeledPoint(label, vector)
  }

  /**
   * Convert Row into LabeledPointWithFrequency format required for updates in MLLib code
   */
  def valuesAsLabeledPointWithFrequency(labelColumnName: String,
                                        featureColumnNames: List[String],
                                        frequencyColumnName: Option[String]): LabeledPointWithFrequency = {
    val label = DataTypes.toDouble(self.value(labelColumnName))
    val vector = valuesAsDenseVector(featureColumnNames)

    val frequency = frequencyColumnName match {
      case Some(freqColumn) => DataTypes.toDouble(self.value(freqColumn))
      case _ => 1d
    }
    new LabeledPointWithFrequency(label, vector, frequency)
  }
}
