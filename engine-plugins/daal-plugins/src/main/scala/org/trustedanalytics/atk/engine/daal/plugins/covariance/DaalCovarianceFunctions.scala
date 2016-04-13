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

package org.trustedanalytics.atk.engine.daal.plugins.covariance

import breeze.numerics.abs
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.daal.plugins.tables.DistributedNumericTable
import com.intel.daal.algorithms.covariance._

/**
 * Object for calculating covariance matrix using Intel DAAL
 */
object DaalCovarianceFunctions extends Serializable {

  /**
   * Compute Variance-Covariance or Correlation matrix for two or more columns using Intel DAAL
   *
   * @param frameRdd input rdd containing all columns
   * @param dataColumnNames column names for which we calculate the covariance matrix
   * @param matrixType Type of matrix to compute (Variance-Covariance or Correlation matrix)
   * @param outputVectorLength If specified, output results as a column of type 'vector(vectorOutputLength)'
   * @return the covariance matrix in a RDD[Rows]
   */
  def covarianceMatrix(frameRdd: FrameRdd,
                       dataColumnNames: List[String],
                       matrixType: ResultId,
                       outputVectorLength: Option[Long] = None): RDD[Row] = {

    val table = DistributedNumericTable.createTable(frameRdd, dataColumnNames)
    val covarianceMatrix = DaalCovarianceAlgorithm(table).computeCovariance(matrixType)

    val formatter: Array[Any] => Array[Any] = outputVectorLength match {
      case Some(length) =>
        val vectorizer = DataTypes.toVector(length)_
        x => Array(vectorizer(x))
        case _ => identity
    }

    val arrGenericRow = covarianceMatrix.map(row => {
      val formattedRow: Array[Any] = formatter(row.map(x => if (x.isNaN || abs(x) < .000001) 0 else x))
      new GenericRow(formattedRow)
    })

    frameRdd.sparkContext.parallelize(arrGenericRow)
  }
}
