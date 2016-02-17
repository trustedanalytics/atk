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
package org.trustedanalytics.atk.engine.daal.plugins.pca

import com.intel.daal.algorithms.PartialResult
import com.intel.daal.algorithms.pca._
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.daal.plugins.DistributedNumericTable

object DaalPcaFunctions extends Serializable {

  /**
   * Run DAAL Principal Components Algorithm (PCA)
   *
   * @param frameRdd Input frame
   * @param arguments PCA arguments
   * @return PCA results with eigen values and vectors
   */
  def runPCA(frameRdd: FrameRdd, arguments: DaalPcaArgs): DaalPcaResult = {
    val distributedTable = new DistributedNumericTable(frameRdd, arguments.columnNames)
    val partialResults = computePcaPartialResults(distributedTable, arguments)
    val pcaResults = mergePcaPartialResults(partialResults, arguments)
    pcaResults
  }

  /**
   * Compute partial PCA results locally
   *
   * This function is run once for each Spark partition
   *
   * @param distributedTable Input table
   * @param arguments PCA arguments
   * @return Partial PCA results
   */
  private def computePcaPartialResults(distributedTable: DistributedNumericTable, arguments: DaalPcaArgs): RDD[PartialResult] = {
    distributedTable.rdd.map(tableWithIndex => {
      val context = new DaalContext
      val pcaLocal = new DistributedStep1Local(context, classOf[java.lang.Double], arguments.getPcaMethod())
      pcaLocal.input.set(InputId.data, tableWithIndex.getUnpackedTable(context))
      val partialResult = pcaLocal.compute
      partialResult.pack
      context.dispose
      partialResult
    })
  }

  /**
   * Merge partial PCA results at Spark master
   *
   * @param partsRDD Partial PCA results
   * @param arguments PCA arguments
   * @return PCA results with eigen values and vectors
   */
  private def mergePcaPartialResults(partsRDD: RDD[PartialResult], arguments: DaalPcaArgs): DaalPcaResult = {
    val parts_List = partsRDD.collect()
    val context = new DaalContext
    val pcaMaster: DistributedStep2Master = new DistributedStep2Master(context, classOf[java.lang.Double], arguments.getPcaMethod())
    for (value <- parts_List) {
      value.unpack(context)
      pcaMaster.input.add(MasterInputId.partialResults, value)
    }
    pcaMaster.compute
    val result = new DaalPcaResult(pcaMaster.finalizeCompute)
    context.dispose()
    result
  }

}
