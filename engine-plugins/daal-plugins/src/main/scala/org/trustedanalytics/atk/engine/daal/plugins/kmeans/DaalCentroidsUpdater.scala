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
package org.trustedanalytics.atk.engine.daal.plugins.kmeans

import com.intel.daal.algorithms.kmeans._
import com.intel.daal.services.DaalContext
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.daal.plugins.DistributedAlgorithm
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }

/**
 * Run one iteration of k-means clustering algorithm to update cluster centroids
 *
 * @param featureTable Feature table
 * @param centroids Input cluster centroids
 * @param labelColumn Name of output column with index of cluster each observation belongs to
 */
case class DaalCentroidsUpdater(featureTable: DistributedNumericTable,
                                centroids: IndexedNumericTable,
                                labelColumn: String)
    extends DistributedAlgorithm[PartialResult, Result] {

  /**
   * Run one iteration of k-means clustering algorithm to update cluster centroids
   *
   * @return Updated centroids
   */
  def updateCentroids(): IndexedNumericTable = {
    val context = new DaalContext
    val partialResults = computePartialResults()
    val results = mergePartialResults(context, partialResults)
    val updatedCentroids = IndexedNumericTable(0.toLong, results.get(ResultId.centroids))
    context.dispose()
    updatedCentroids
  }

  /**
   * Partially update cluster centroids on each Spark partition
   *
   * @return RDD of partial k-means and optional cluster assignments
   */
  override def computePartialResults(): RDD[PartialResult] = {
    featureTable.rdd.map { table =>
      val context = new DaalContext
      val local = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense, centroids.numRows)
      local.input.set(InputId.data, table.getUnpackedTable(context))
      local.input.set(InputId.inputCentroids, centroids.getUnpackedTable(context))
      local.parameter.setAssignFlag(false)
      val partialResult = local.compute
      partialResult.pack()

      context.dispose()
      partialResult
    }
  }

  /**
   * Merge partial results of K-means clustering to compute cluster centroids
   *
   * @param context DAAL context
   * @param partsRdd RDD of partial results
   * @return Updated cluster centroids
   */
  override def mergePartialResults(context: DaalContext, partsRdd: RDD[PartialResult]): Result = {
    val partialResults = partsRdd.collect()
    val master = new DistributedStep2Master(context, classOf[java.lang.Double], Method.defaultDense, centroids.numRows)

    for (value <- partialResults) {
      value.unpack(context)
      master.input.add(DistributedStep2MasterInputId.partialResults, value)
    }
    master.compute

    val result = master.finalizeCompute
    result
  }

}

