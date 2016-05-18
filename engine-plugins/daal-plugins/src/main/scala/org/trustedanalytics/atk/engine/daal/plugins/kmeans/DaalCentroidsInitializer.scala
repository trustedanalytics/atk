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

import com.intel.daal.algorithms.kmeans.init._
import com.intel.daal.services.DaalContext
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils.withDaalContext
import org.trustedanalytics.atk.engine.daal.plugins.DistributedAlgorithm
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ IndexedNumericTable, DistributedNumericTable }

/**
 * Initializer of cluster centroids in DAAL KMeans
 *
 * @param featureTable Feature table
 * @param args Training arguments
 */
case class DaalCentroidsInitializer(featureTable: DistributedNumericTable,
                                    args: DaalKMeansTrainArgs)
    extends DistributedAlgorithm[InitPartialResult, InitResult] {

  /**
   * Initialize cluster centroids using DAAL KMeans clustering
   *
   * @return Numeric table with initial cluster centroids
   */
  def initializeCentroids(): IndexedNumericTable = {
    withDaalContext { context =>
      val partsRdd = computePartialResults()
      val results = mergePartialResults(context, partsRdd)
      IndexedNumericTable(0L, results.get(InitResultId.centroids))
    }.elseError("Could not initialize centroids")
  }

  /**
   * Compute initial cluster centroids locally
   *
   * @return Partial results of centroid initialization
   */
  override def computePartialResults(): RDD[InitPartialResult] = {
    val totalRows = featureTable.numRows
    featureTable.rdd.map { table =>
      withDaalContext { context =>
        val initLocal = new InitDistributedStep1Local(context, classOf[java.lang.Double],
          args.getInitMethod, args.k.toLong, totalRows, table.index)
        initLocal.input.set(InitInputId.data, table.getUnpackedTable(context))
        val partialResult = initLocal.compute
        partialResult.pack()
        partialResult
      }.elseError("Could not compute partial results for centroid initialization")
    }
  }

  /**
   * Merge partial results of cluster initialiation at Spark master to create initial cluster centroids
   *
   * @param context DAAL context
   * @param partsRdd Partial results of centroid initialization
   * @return Numeric table with initial cluster centroids
   */
  override def mergePartialResults(context: DaalContext, partsRdd: RDD[InitPartialResult]): InitResult = {
    val partsCollection = partsRdd.collect()
    val initMaster: InitDistributedStep2Master = new InitDistributedStep2Master(context,
      classOf[java.lang.Double], args.getInitMethod, args.k.toLong)

    for (value <- partsCollection) {
      value.unpack(context)
      initMaster.input.add(InitDistributedStep2MasterInputId.partialResults, value)
    }
    initMaster.compute

    val result = initMaster.finalizeCompute
    result
  }

}
