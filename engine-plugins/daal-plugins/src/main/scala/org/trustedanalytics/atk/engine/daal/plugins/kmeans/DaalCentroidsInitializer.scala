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
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ IndexedNumericTable, DistributedNumericTable }

/**
 * Initializer of cluster centroids in DAAL KMeans
 *
 * @param featureTable Feature table
 * @param args Training arguments
 */
case class DaalCentroidsInitializer(featureTable: DistributedNumericTable,
                                    args: DaalKMeansTrainArgs) {
  /**
   * Initialize cluster centroids using DAAL KMeans clustering
   *
   * @return Numeric tbale with initial cluster centroids
   */
  def initializeCentroids(): IndexedNumericTable = {
    // Compute partial results on local node, and merge results on master
    val partsRdd = initializeCentroidsLocal()
    mergeInitialCentroids(partsRdd)
  }

  /**
   * Compute initial cluster centroids locally for each Spark partition
   *
   * @return Partial results of centroid initialization
   */
  private def initializeCentroidsLocal(): RDD[InitPartialResult] = {
    val totalRows = featureTable.getNumRows
    featureTable.rdd.map { table =>
      val context = new DaalContext
      val initLocal = new InitDistributedStep1Local(context, classOf[java.lang.Double], args.getInitMethod, args.k.toLong, totalRows, table.index)
      initLocal.input.set(InitInputId.data, table.getUnpackedTable(context))
      val partialResult = initLocal.compute
      partialResult.pack()
      context.dispose()
      partialResult
    }
  }

  /**
   * Merge partial results of cluster initialiation at Spark master to create initial cluster centroids
   *
   * @param partsRdd Partial results of centroid initialization
   * @return Numeric table with initial cluster centroids
   */
  private def mergeInitialCentroids(partsRdd: RDD[InitPartialResult]): IndexedNumericTable = {
    val daalContext = new DaalContext()
    val partsCollection = partsRdd.collect()
    val initMaster: InitDistributedStep2Master = new InitDistributedStep2Master(daalContext, classOf[java.lang.Double], args.getInitMethod, args.k)
    for (value <- partsCollection) {
      value.unpack(daalContext)
      initMaster.input.add(InitDistributedStep2MasterInputId.partialResults, value)
    }
    initMaster.compute
    val result = initMaster.finalizeCompute
    val centroids = IndexedNumericTable(0.toLong, result.get(InitResultId.centroids))
    daalContext.dispose()
    centroids
  }
}
