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

package org.trustedanalytics.atk.engine.frame.plugins.boxcox

import java.io.Serializable

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.{ VectorFunctions, RowWrapper }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating the box-cox transformation and reverse box-cox transformation
 */
object BoxCoxFunctions extends Serializable {
  /**
   * Compute the Box-cox transformation for each row of the column columnName in the input RDD
   * @param frameRdd Data frame
   * @param columnName The column on the data frame whose box-cox transformation is to be computed
   * @param lambdaValue The lambda power parameter
   * @return Data frame with an additional column storing the box-cox transformed value
   */
  def boxCox(frameRdd: FrameRdd, columnName: String, lambdaValue: Double): RDD[Row] = {
    frameRdd.mapRows(row => {
      val y = DataTypes.toDouble(row.value(columnName))
      val boxCox = computeBoxCoxTransformation(y, lambdaValue)
      new GenericRow(row.valuesAsArray() :+ boxCox)
    })
  }

  /**
   * Compute the reverse Box-cox transformation for each row of the column columnName in the input RDD
   * @param frameRdd Data frame
   * @param columnName The column on the data frame whose reverse box-cox transformation is to be computed
   * @param lambdaValue The lambda power parameter
   * @return Data frame with an additional column storing the reverse box-cox transformed value
   */
  def reverseBoxCox(frameRdd: FrameRdd, columnName: String, lambdaValue: Double): RDD[Row] = {
    frameRdd.mapRows(row => {
      val boxCox = DataTypes.toDouble(row.value(columnName))
      val y = computeReverseBoxCoxTransformation(boxCox, lambdaValue)
      new GenericRow(row.valuesAsArray() :+ y)
    })
  }

  /**
   * Compute the box-cox transformation for the given record.
   * boxcox = log(y); if lambda=0,
   * boxcox = (y^lambda -1)/lambda ; else
   * where log is the natural log
   * @param y The value whose box-cox transformation is to be computed
   * @param lambdaValue The lambda power parameter
   * @return The box-cox transformation of y
   */
  def computeBoxCoxTransformation(y: Double, lambdaValue: Double): Double = {
    val boxCox: Double = if (lambdaValue == 0d) math.log(y) else (math.pow(y, lambdaValue) - 1) / lambdaValue
    boxCox
  }

  /**
   * Compute the reverse box-cox transformation for the given record.
   * y = exp(boxcox); if lambda=0,
   * y = (lambda * boxcox + 1)^(1/lambda) ; else
   * @param boxCox The value whose reverse box-cox transformation is to be computed
   * @param lambdaValue The lambda power parameter
   * @return The reverse box-cox transformation value
   */
  def computeReverseBoxCoxTransformation(boxCox: Double, lambdaValue: Double): Double = {
    val y: Double = if (lambdaValue == 0d) math.exp(boxCox) else math.pow(lambdaValue * boxCox + 1, 1 / lambdaValue)
    y
  }
}
