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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.covariance

import breeze.linalg.DenseVector
import breeze.numerics.abs
import org.trustedanalytics.atk.domain.DoubleValue
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ Vectors, Vector, Matrix }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
 * Object for calculating covariance and the covariance matrix
 */

object CovarianceFunctions extends Serializable {

  /**
   * Compute covariance for exactly two columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance
   * @return covariance wrapped in CovarianceReturn
   */
  def covariance(frameRdd: FrameRdd,
                 dataColumnNames: List[String]): DoubleValue = {

    // compute and return covariance
    def rowMatrix: RowMatrix = new RowMatrix(frameRdd.toDenseVectorRDD(dataColumnNames))

    val covariance: Matrix = rowMatrix.computeCovariance()

    val dblVal: Double = covariance.toArray(1)

    DoubleValue(if (dblVal.isNaN || abs(dblVal) < .000001) 0 else dblVal)
  }

  /**
   * Compute covariance for two or more columns
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance matrix
   * @param outputVectorLength If specified, output results as a column of type 'vector(vectorOutputLength)'
   * @return the covariance matrix in a RDD[Rows]
   */
  def covarianceMatrix(frameRdd: FrameRdd,
                       dataColumnNames: List[String],
                       outputVectorLength: Option[Long] = None): RDD[Row] = {

    def rowMatrix: RowMatrix = new RowMatrix(frameRdd.toDenseVectorRDD(dataColumnNames))

    val covariance: Matrix = rowMatrix.computeCovariance()
    val vecArray = covariance.toArray.grouped(covariance.numCols).toArray
    val formatter: Array[Any] => Array[Any] = outputVectorLength match {
      case Some(length) =>
        val vectorizer = DataTypes.toVector(length)_
        x => Array(vectorizer(x))
        case _ => identity
    }

    val arrGenericRow = vecArray.map(row => {
      val formattedRow: Array[Any] = formatter(row.map(x => if (x.isNaN || abs(x) < .000001) 0 else x))
      new GenericRow(formattedRow)
    })

    frameRdd.sparkContext.parallelize(arrGenericRow)
  }
}
