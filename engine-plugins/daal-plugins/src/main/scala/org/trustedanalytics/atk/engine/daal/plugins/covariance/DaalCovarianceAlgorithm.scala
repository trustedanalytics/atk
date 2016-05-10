/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.atk.engine.daal.plugins.covariance

import com.intel.daal.algorithms.covariance._
import com.intel.daal.services.DaalContext
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils.withDaalContext
import org.trustedanalytics.atk.engine.daal.plugins.DistributedAlgorithm
import org.trustedanalytics.atk.engine.daal.plugins.tables.DaalConversionImplicits._
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }

/**
 * Distributed algorithm for computing covariance matrix with Intel DAAL
 *
 * @param featureTable Feature table
 */
case class DaalCovarianceAlgorithm(featureTable: DistributedNumericTable)
    extends DistributedAlgorithm[PartialResult, Result] {

  /**
   * Compute Variance-covariance matrix or correlation matrix
   *
   * Uses Intel DAAL default performance-oriented method for dense matrix
   *
   * @param matrixType Type of matrix to compute (Variance-Covariance or Correlation matrix)
   *
   * @return Variance-covariance matrix or correlation matrix
   */
  def computeCovariance(matrixType: ResultId): Array[Array[Double]] = {
    withDaalContext { context =>
      val partialResults = computePartialResults()
      val results = mergePartialResults(context, partialResults)
      val covarianceTable = IndexedNumericTable(0.toLong, results.get(matrixType))
      covarianceTable.getUnpackedTable(context).toArrayOfDoubleArray()
    }.elseError("Could not compute covariance matrix")
  }

  /**
   * Compute partial results for covariance
   *
   * @return RDD of partial results
   */
  override def computePartialResults(): RDD[PartialResult] = {
    featureTable.rdd.map { table =>
      withDaalContext { context =>
        val local = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense)
        local.input.set(InputId.data, table.getUnpackedTable(context))
        val partialResult = local.compute
        partialResult.pack()
        partialResult
      }.elseError("Could not compute partial results for covariance matrix ")
    }
  }

  /**
   * Merge partial covariance results on Spark driver to generate final result
   *
   * @param context DAAL Context
   * @param rdd RDD of partial results
   * @return Final result with covariance matrix
   */
  override def mergePartialResults(context: DaalContext, rdd: RDD[PartialResult]): Result = {
    val partialResults = rdd.collect()
    val master = new DistributedStep2Master(context, classOf[java.lang.Double], Method.defaultDense)

    for (value <- partialResults) {
      value.unpack(context)
      master.input.add(DistributedStep2MasterInputId.partialResults, value)
    }
    master.compute

    val result = master.finalizeCompute
    result
  }
}
