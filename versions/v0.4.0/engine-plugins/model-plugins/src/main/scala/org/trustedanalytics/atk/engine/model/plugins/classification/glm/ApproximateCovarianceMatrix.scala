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
package org.trustedanalytics.atk.engine.model.plugins.classification.glm

import breeze.linalg.{ DenseMatrix, inv }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.{ SparkContext, sql }

import scala.util.Try

/**
 * Covariance matrix generated from model's hessian matrix
 *
 * Covariance matrix is the inverse of the Hessian matrix. The Hessian matrix is
 * the second-order partial derivatives of the model's log-likelihood function.
 *
 * @param hessianMatrix Hessian matrix
 * @param reorderMatrix If true, reorder the matrix so that the intercept is stored in
 *                      the first row and column instead of the last row and column
 */
case class ApproximateCovarianceMatrix(hessianMatrix: DenseMatrix[Double],
                                       reorderMatrix: Boolean = false) {
  require(hessianMatrix != null, "Hessian matrix must not be null")
  val covarianceMatrix = computeCovarianceMatrix(hessianMatrix)

  /**
   * Convert covariance matrix to Frame RDD with a single row and column for each coefficient
   *
   * @param sparkContext Spark context
   * @param columnNames Column names
   * @return Frame RDD
   */
  def toFrameRdd(sparkContext: SparkContext, columnNames: List[String]): FrameRdd = {
    val schema = FrameSchema(columnNames.map(name => Column(name, DataTypes.float64)))

    val rows: IndexedSeq[Row] = for {
      i <- 0 until covarianceMatrix.rows
      row = covarianceMatrix(i, ::).t.map(x => x: Any)
    } yield new GenericRow(row.toArray)

    val rdd = sparkContext.parallelize(rows)
    new FrameRdd(schema, rdd)
  }

  /** Compute covariance matrix from model's hessian matrix */
  private def computeCovarianceMatrix(hessianMatrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val covarianceMatrix: DenseMatrix[Double] = Try(inv(hessianMatrix)).getOrElse({
      throw new scala.IllegalArgumentException("Could not compute covariance matrix: Hessian matrix is not invertable")
    })
    if (reorderMatrix) reorderMatrix(covarianceMatrix) else covarianceMatrix
  }

  /**
   * Reorder the matrix so that the intercept is stored in the first row and first column (standard convention)
   * MLlib models store the intercept in the last row and last column of the matrix
   */
  private def reorderMatrix(matrix: DenseMatrix[Double]): DenseMatrix[Double] = {
    val rows = matrix.rows
    val cols = matrix.cols
    if (rows > 1 && cols > 1) {
      var i = 0
      var j = 0
      var head = 0 //use the first row of matrix as a temporary buffer

      while (i < rows) {
        while (j < cols) {
          val ni = (i + 1) % rows
          val nj = (j + i + 1) % cols
          val tmp = matrix(ni, nj)
          matrix(ni, nj) = matrix(0, head)
          matrix(0, head) = tmp
          head = (head + 1) % cols
          j = j + 1
        }
        i = i + 1
        j = 0
      }
    }
    matrix
  }
}
