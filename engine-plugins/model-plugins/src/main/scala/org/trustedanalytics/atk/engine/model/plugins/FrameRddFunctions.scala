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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ VectorUDT, DenseVector }
import org.apache.spark.mllib.regression.{ LabeledPoint, LabeledPointWithFrequency }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{ DoubleType, StructField, StructType }
import org.trustedanalytics.atk.domain.schema.DataTypes

/**
 * Functions for extending frames with model-related methods.
 * <p>
 * This is best used by importing ModelPluginImplicits._
 * </p>
 * @param self input that these functions are applicable to
 */
class FrameRddFunctions(self: FrameRdd) {
  /**
   * Convert FrameRdd into RDD[LabeledPoint] format required by MLLib
   */
  def toLabeledPointRDD(labelColumnName: String, featureColumnNames: List[String]): RDD[LabeledPoint] = {
    self.mapRows(row => {
      val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
      new LabeledPoint(DataTypes.toDouble(row.value(labelColumnName)), new DenseVector(features.toArray))
    })
  }

  /**
   * Convert FrameRdd into RDD[LabeledPointWithFrequency] format required for updates in MLLib code
   */
  def toLabeledPointRDDWithFrequency(labelColumnName: String,
                                     featureColumnNames: List[String],
                                     frequencyColumnName: Option[String]): RDD[LabeledPointWithFrequency] = {
    self.mapRows(row => {
      val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
      frequencyColumnName match {
        case Some(freqColumn) => {
          new LabeledPointWithFrequency(DataTypes.toDouble(row.value(labelColumnName)),
            new DenseVector(features.toArray), DataTypes.toDouble(row.value(freqColumn)))
        }
        case _ => {
          new LabeledPointWithFrequency(DataTypes.toDouble(row.value(labelColumnName)),
            new DenseVector(features.toArray), DataTypes.toDouble(1.0))
        }
      }
    })
  }

}