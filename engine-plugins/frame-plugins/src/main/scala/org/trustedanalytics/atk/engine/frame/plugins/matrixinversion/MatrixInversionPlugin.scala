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

package org.trustedanalytics.atk.engine.frame.plugins.matrixinversion

import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{ DenseVector, DenseMatrix }
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ MatrixInversionArgs, FrameReference }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }

import scala.collection.mutable.ListBuffer

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Inverts a matrix.
 *
 */
@PluginDoc(oneLine = "Inverts a matrix.",
  extended = """Inverts a matrix.""",
  returns = "A pseudo-inverted matrix or error if the matrix is singular (cannot be inverted).")
class MatrixInversionPlugin extends SparkCommandPlugin[MatrixInversionArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/invert_matrix"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: MatrixInversionArgs)(implicit invocation: Invocation) = 3

  /**
   * Calculate the pseudo-inverted of a matrix.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type
   */
  override def execute(arguments: MatrixInversionArgs)(implicit invocation: Invocation): FrameReference = {
    val frameStorage = engine.frames
    val frame: SparkFrame = arguments.frame
    val columnNames = arguments.columnNames
    val inputSchema = frame.schema

    // run the operation
    val rowMatrix = new RowMatrix(frame.rdd.toDenseVectorRDD(columnNames))
    val invertedMatrix = computeInverse(rowMatrix)

    // save results
    val invertedMatrixRdd = toFrameRdd(sc, invertedMatrix)
    frameStorage.tryNewFrame(CreateEntityArgs(description = Some("created by invert_matrix command"))) {
      newFrame => newFrame.save(new FrameRdd(inputSchema, invertedMatrixRdd))
    }
  }

  /**
   * Inverts a matrix
   * @param matrix the input matrix
   * @return the pseudo-inverted matrix or error if matrix cannot be inverted
   */
  private def computeInverse(matrix: RowMatrix): DenseMatrix = {

    val columns = matrix.numCols.toInt
    val svd = matrix.computeSVD(columns, computeU = true)
    if (svd.s.size < columns) {
      throw  new RuntimeException(s"MatrixInversionPlugin computeInverse cannot be called on a singular matrix")
    }

    // Create the inv diagonal matrix from svd.s
    val invSigma = DenseMatrix.diag(new DenseVector(svd.s.toArray.map(x => math.pow(x, -1))))
    val U = new DenseMatrix(svd.U.numRows().toInt, svd.U.numCols().toInt, svd.U.rows.collect.flatMap(x => x.toArray))
    val V = svd.V

    // formula (note: U is already transposed so we just multiply with it):
    //
    //  inv(X) = V * inv(Sigma) * transpose(U)
    //
    (V.multiply(invSigma)).multiply(U)
  }

  /**
   * Convert matrix to Frame RDD
   *
   * @param sparkContext Spark context
   * @param matrix a dense matrix
   * @return an equivalent frame RDD
   */
  private def toFrameRdd(sparkContext: SparkContext,
                         matrix: DenseMatrix): RDD[Row] = {

    val numRows = matrix.numRows
    val numCols = matrix.numCols
    val rowBuffer = new ListBuffer[Row]()

    for (i <- 0 until numRows) {
      val rowArray = new Array[Any](numCols)
      for (j <- 0 until numCols) {
        rowArray(j) = matrix(i, j)
      }
      rowBuffer += new GenericRow(rowArray)
    }

    sparkContext.parallelize(rowBuffer)
  }
}
