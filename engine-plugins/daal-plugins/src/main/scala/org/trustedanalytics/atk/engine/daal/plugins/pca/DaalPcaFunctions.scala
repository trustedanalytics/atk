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
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.trustedanalytics.atk.engine.daal.plugins.conversions.DaalConversionImplicits
import DaalConversionImplicits._
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD

object DaalPcaFunctions extends Serializable {

  def runPCA(frameRdd: FrameRdd, arguments: DaalPcaArgs): DaalPcaResult = {
    val pcaMethod = arguments.getPcaMethod()
    val context = new DaalContext()
    val dataRdd = frameRdd.toNumericTableRdd(arguments.columnNames)

    val partialResults = computePcaPartialResults(dataRdd, pcaMethod)
    val pcaResults = mergePcaPartialResults(context, partialResults, pcaMethod)

    context.dispose()
    pcaResults
  }

  private def computePcaPartialResults(dataRdd: RDD[(Integer, HomogenNumericTable)], pcaMethod: Method): RDD[(Integer, PartialResult)] = {
    dataRdd.map {
      case (tableId, table) =>
        val context = new DaalContext
        val pcaLocal = new DistributedStep1Local(context, classOf[java.lang.Double], pcaMethod)
        table.unpack(context)
        pcaLocal.input.set(InputId.data, table)
        val partialResult = pcaLocal.compute
        partialResult.pack
        context.dispose
        (tableId, partialResult)
    }
  }

  private def mergePcaPartialResults(context: DaalContext, partsRDD: RDD[(Integer, PartialResult)], pcaMethod: Method): DaalPcaResult = {
    val pcaMaster: DistributedStep2Master = new DistributedStep2Master(context, classOf[java.lang.Double], Method.correlationDense)
    val parts_List = partsRDD.collect()
    for (value <- parts_List) {
      value._2.unpack(context)
      pcaMaster.input.add(MasterInputId.partialResults, value._2)
    }
    pcaMaster.compute
    val res = pcaMaster.finalizeCompute
    DaalPcaResult(res.get(ResultId.eigenValues), res.get(ResultId.eigenVectors))
  }
}
