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

import com.intel.daal.algorithms.kmeans.{ ResultId, InputId, Method, DistributedStep1Local }
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils.withDaalContext
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }

/**
 * Assign cluster index to each observation in feature table
 *
 * @param featureTable Feature table
 * @param centroids Cluster centroids
 * @param labelColumn Name of output column with index of cluster each observation belongs to
 */
case class DaalClusterAssigner(featureTable: DistributedNumericTable,
                               centroids: IndexedNumericTable,
                               labelColumn: String) {

  /**
   * Assign cluster index to each observation in feature table
   *
   * @return Frame of cluster assignments
   */
  def assign(): FrameRdd = {
    val schema = FrameSchema(List(Column(labelColumn, DataTypes.int32)))
    var numRows = 0L
    val rdd = featureTable.rdd.map { table =>
      withDaalContext { context =>
        val local = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense, centroids.numRows)
        local.input.set(InputId.data, table.getUnpackedTable(context))
        local.input.set(InputId.inputCentroids, centroids.getUnpackedTable(context))
        local.parameter.setAssignFlag(true)
        val partialResults = local.compute
        partialResults.pack()

        val result = local.finalizeCompute()
        val assignmentTable = result.get(ResultId.assignments).asInstanceOf[HomogenNumericTable]
        val assignments = IndexedNumericTable(table.index, assignmentTable)
        numRows += assignments.numRows
        assignments
      }.elseError("Could not assign cluster centroids")
    }

    DistributedNumericTable(rdd, numRows).toFrameRdd(schema)
  }

  /**
   * Compute size of predicted clusters
   *
   * @param assignmentFrame Frame with cluster assignments
   * @return Map of cluster names and sizes
   */
  def clusterSizes(assignmentFrame: FrameRdd): Map[String, Long] = {
    //TODO: Use DAAL partial results nObservations to compute cluster sizes
    assignmentFrame.mapRows(row => {
      val clusterId = row.intValue(labelColumn)
      ("Cluster:" + clusterId.toString, 1L)
    }).reduceByKey(_ + _).collect().toMap
  }

}
