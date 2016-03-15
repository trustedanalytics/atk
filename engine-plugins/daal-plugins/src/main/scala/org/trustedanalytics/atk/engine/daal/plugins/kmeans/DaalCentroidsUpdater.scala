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
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ IndexedNumericTable, DistributedNumericTable }

/**
 * Run one iteration of k-means clustering algorithm to update cluster centroids
 *
 * @param featureTable Feature table
 * @param inputCentroids Input centroids*
 * @param labelColumn Name of output column with index of cluster each observation belongs to
 * @param assignFlag If true, create table with cluster assignments
 */
case class DaalCentroidsUpdater(featureTable: DistributedNumericTable,
                                inputCentroids: IndexedNumericTable,
                                labelColumn: String,
                                assignFlag: Boolean = false) {
  /**
   * Run one iteration of k-means clustering algorithm to update cluster centroids
   *
   * @return Updated centroids, and optional table of cluster assignments
   */
  def updateCentroids(): DaalKMeansResults = {
    val partialResults = updateCentroidsLocal()
    val daalContext = new DaalContext()
    val results = mergeClusterCentroids(daalContext, partialResults.keys)
    val assignments = getClusterAssignments(partialResults)
    val centroids = IndexedNumericTable(0.toLong, results.get(ResultId.centroids))
    daalContext.dispose()
    DaalKMeansResults(centroids, centroids.numRows, assignments)
  }

  /**
   * Partially update cluster centroids on each Spark partition
   *
   * @return RDD of partial k-means and optional cluster assignments
   */
  private def updateCentroidsLocal(): RDD[(PartialResult, Option[IndexedNumericTable])] = {
    val partialResultsRdd = featureTable.rdd.map { table =>
      val context = new DaalContext
      val local = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense, inputCentroids.numRows)
      local.input.set(InputId.data, table.getUnpackedTable(context))
      local.input.set(InputId.inputCentroids, inputCentroids.getUnpackedTable(context))
      local.parameter.setAssignFlag(assignFlag)
      val partialResult = local.compute
      partialResult.pack()

      val assignments = if (assignFlag) {
        val result = local.finalizeCompute()
        val assignmentTable: HomogenNumericTable = result.get(ResultId.assignments).asInstanceOf[HomogenNumericTable]
        Some(IndexedNumericTable(table.index, assignmentTable))
      }
      else {
        None
      }

      context.dispose()
      (partialResult, assignments)
    }
    partialResultsRdd
  }

  /**
   * Merge partial results of K-means clustering to compute cluster centroids
   *
   * @param daalContext DAAL context
   * @param partsRdd RDD of partial results
   * @return Updated cluster centroids
   */
  private def mergeClusterCentroids(daalContext: DaalContext, partsRdd: RDD[PartialResult]): Result = {
    val partialResults = partsRdd.collect()
    val master = new DistributedStep2Master(daalContext, classOf[java.lang.Double], Method.defaultDense, inputCentroids.numRows)

    for (value <- partialResults) {
      value.unpack(daalContext)
      master.input.add(DistributedStep2MasterInputId.partialResults, value)
    }
    master.compute
    val result = master.finalizeCompute
    result
  }

  /**
   * Get table with cluster assignments
   *
   * @param partialResults RDD of partial k-means and optional cluster assignments
   * @return Optional frame of cluster assignments
   */
  def getClusterAssignments(partialResults: RDD[(PartialResult, Option[IndexedNumericTable])]): Option[FrameRdd] = {
    val assignments = if (assignFlag) {
      val rdd = partialResults.values.map(_.getOrElse(
        throw new scala.RuntimeException("Cluster assignment table cannot be empty")))
      val numRows = rdd.map(table => table.numRows.toLong).reduce(_ + _)
      val assignmentTable = new DistributedNumericTable(rdd, numRows)
      val schema = FrameSchema(List(Column(labelColumn, DataTypes.int32)))
      Some(assignmentTable.toFrameRdd(schema))
    }
    else {
      None
    }
    assignments
  }
}
