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

import com.intel.daal.algorithms.pca._
import com.intel.daal.algorithms.PartialResult
import com.intel.daal.services.DaalContext
import org.trustedanalytics.atk.engine.daal.plugins.DistributedNumericTable
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD

object DaalPcaFunctions extends Serializable {

  def runPCA(frameRdd: FrameRdd, arguments: DaalPcaArgs): DaalPcaResult = {
    val distributedTable = new DistributedNumericTable(frameRdd, arguments.columnNames)
    val partialResults = computePcaPartialResults(distributedTable, arguments)
    val pcaResults = mergePcaPartialResults(partialResults, arguments)
    pcaResults
  }

  private def computePcaPartialResults(distributedTable: DistributedNumericTable, arguments: DaalPcaArgs): RDD[PartialResult] = {
    distributedTable.rdd.map(tableWithIndex => {
      val context = new DaalContext
      val pcaLocal = new DistributedStep1Local(context, classOf[java.lang.Double], arguments.getPcaMethod())
      pcaLocal.input.set(InputId.data, tableWithIndex.getTable(context))
      val partialResult = pcaLocal.compute
      partialResult.pack
      context.dispose
      partialResult
    })
  }

  private def mergePcaPartialResults(partsRDD: RDD[PartialResult], arguments: DaalPcaArgs): DaalPcaResult = {
    val context = new DaalContext
    val pcaMaster: DistributedStep2Master = new DistributedStep2Master(context, classOf[java.lang.Double], arguments.getPcaMethod())
    val parts_List = partsRDD.collect()
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
