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
package org.trustedanalytics.atk.engine.daal.plugins.kmeans

import java.lang

import com.intel.daal.algorithms.kmeans._
import com.intel.daal.algorithms.kmeans.init._
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.daal.plugins.conversions.DaalConversionImplicits._
import org.trustedanalytics.atk.engine.daal.plugins.{ DistributedNumericTable, IndexedNumericTable }

object DaalKMeansFunctions extends Serializable {

  /**
   * Train K-means clustering model
   * @param frameRdd Input frame
   * @param args Input arguments
   * @return Trained k-means model
   */
  def trainKMeansModel(frameRdd: FrameRdd,
                       args: DaalKMeansTrainArgs): DaalKMeansModelData = {
    val sparkContext = frameRdd.sparkContext
    val daalContext = new DaalContext()
    val table = new DistributedNumericTable(frameRdd, args.observationColumns)
    table.cache()
    var result: Result = null
    var centroids = initializeCentroids(sparkContext, daalContext, table, args)

    for (i <- 1 until args.maxIterations) {
      result = updateCentroids(sparkContext, daalContext, table, centroids, args)
      centroids = getCentroids(result)
    }

    val finalResults = getClusterAssignments(sparkContext, daalContext, table, centroids, args)
    centroids = getCentroids(finalResults._1)
    val assignments = finalResults._2.flatMap(table => {
      val context = new DaalContext
      val rows = table.toRowIter(context)
      context.dispose()
      rows
    })

    val schema = FrameSchema(List(Column("cluster", DataTypes.float64)))
    val assignmentFrame = frameRdd.zipFrameRdd(new FrameRdd(schema, assignments))

    val modelData = DaalKMeansModelData(centroids.getUnpackedTable(daalContext).toArrayOfDoubleArray(), args.k, assignmentFrame)
    table.unpersist()
    daalContext.dispose()
    modelData
  }

  /**
   * Get cluster centroids from k-means result
   *
   * @param result k-means result
   * @return Cluster centroids
   */
  def getCentroids(result: Result): IndexedNumericTable = {
    IndexedNumericTable(0.toLong, result.get(ResultId.centroids))
  }

  /**
   * Get goal function for k-means
   *
   * The goal function is the sum of distances of observations to their closest cluster center
   *
   * @param result k-means result
   * @return sum of distances of observations to their closest cluster center
   */
  def getGoalFunction(result: Result): Double = {
    val goal = result.get(ResultId.goalFunction).asInstanceOf[HomogenNumericTable]
    val arr = goal.getDoubleArray
    if (arr.size > 0) {
      arr(0)
    }
    else {
      throw new RuntimeException("Unable to calculate goal function for k-means clustering")
    }
  }

  /**
   * Get cluster assignments for each observation in feature table
   *
   * @param sparkContext Spark context
   * @param daalContext DAAL context
   * @param featureTable Feature table
   * @param inputCentroids Cluster centroids
   * @param args Input arguments
   * @return Table with cluster assignments for each observation
   */
  def getClusterAssignments(sparkContext: SparkContext,
                            daalContext: DaalContext,
                            featureTable: DistributedNumericTable,
                            inputCentroids: IndexedNumericTable,
                            args: DaalKMeansTrainArgs): (Result, RDD[IndexedNumericTable]) = {
    val partialResults = updateCentroidsLocal(featureTable, inputCentroids, args, true)
    val centroids = mergeClusterCentroids(daalContext, inputCentroids, args, partialResults.keys)
    val assignments = partialResults.values
    (centroids, assignments)
  }

  /**
   * Initialize cluster centroids
   *
   * @param sparkContext Spark context
   * @param daalContext Daal context
   * @param featureTable Feature table
   * @param args Input arguments
   * @return Numeric tbale with initial cluster centroids
   */
  private def initializeCentroids(sparkContext: SparkContext,
                                  daalContext: DaalContext,
                                  featureTable: DistributedNumericTable,
                                  args: DaalKMeansTrainArgs): IndexedNumericTable = {
    // Compute partial results on local node, and merge results on master
    val partsRdd = initializeCentroidsLocal(featureTable, args)
    mergeInitialCentroids(daalContext, args, partsRdd)
  }

  /**
   * Compute initial cluster centroids locally
   *
   * This function is run once for each Spark partition
   *
   * @param featureTable Feature table
   * @param args Input arguments
   * @return Partial results of centroid initialization
   */
  private def initializeCentroidsLocal(featureTable: DistributedNumericTable, args: DaalKMeansTrainArgs): RDD[InitPartialResult] = {
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
   * @param daalContext DAAL context
   * @param args Input arguments
   * @param partsRdd Partial results of centroid initialization
   * @return Numeric table with initial cluster centroids
   */
  private def mergeInitialCentroids(daalContext: DaalContext, args: DaalKMeansTrainArgs, partsRdd: RDD[InitPartialResult]): IndexedNumericTable = {
    val partsCollection = partsRdd.collect()
    val initMaster: InitDistributedStep2Master = new InitDistributedStep2Master(daalContext, classOf[java.lang.Double], args.getInitMethod, args.k)
    for (value <- partsCollection) {
      value.unpack(daalContext)
      initMaster.input.add(InitDistributedStep2MasterInputId.partialResults, value)
    }
    initMaster.compute
    val result = initMaster.finalizeCompute
    IndexedNumericTable(0.toLong, result.get(InitResultId.centroids))
  }

  /**
   * Run one iteration of k-means clustering algorithm to update cluster centroids
   *
   * @param sparkContext Spark context
   * @param daalContext DAAL context
   * @param featureTable Feature table
   * @param inputCentroids Input centroids
   * @param args Input arguments
   * @return Result object with updated centroids
   */
  private def updateCentroids(sparkContext: SparkContext,
                              daalContext: DaalContext,
                              featureTable: DistributedNumericTable,
                              inputCentroids: IndexedNumericTable,
                              args: DaalKMeansTrainArgs): Result = {
    val partialResults = updateCentroidsLocal(featureTable, inputCentroids, args)
    mergeClusterCentroids(daalContext, inputCentroids, args, partialResults.keys)
  }

  /**
   *
   * @param featureTable
   * @param inputCentroids
   * @param args
   * @param assignFlag
   * @return
   */
  def updateCentroidsLocal(featureTable: DistributedNumericTable,
                           inputCentroids: IndexedNumericTable,
                           args: DaalKMeansTrainArgs,
                           assignFlag: Boolean = false): RDD[(PartialResult, IndexedNumericTable)] = {
    // compute partial results at slaves
    featureTable.rdd.map { table =>
      val context = new DaalContext
      val local = new DistributedStep1Local(context, classOf[lang.Double], args.getClusteringMethod, inputCentroids.numRows)
      local.input.set(InputId.data, table.getUnpackedTable(context))
      local.input.set(InputId.inputCentroids, inputCentroids.getUnpackedTable(context))
      local.parameter.setAssignFlag(assignFlag)
      val partialResult = local.compute
      partialResult.pack()

      val assignments = if (assignFlag) {
        val result = local.finalizeCompute()
        val assignmentTable: HomogenNumericTable = result.get(ResultId.assignments).asInstanceOf[HomogenNumericTable]
        IndexedNumericTable(table.index, assignmentTable)
      }
      else {
        null
      }

      context.dispose()
      (partialResult, assignments)
    }
  }

  /**
   *
   * @param daalContext
   * @param inputCentroids
   * @param args
   * @param partsRdd
   * @return
   */
  def mergeClusterCentroids(daalContext: DaalContext, inputCentroids: IndexedNumericTable, args: DaalKMeansTrainArgs, partsRdd: RDD[PartialResult]): Result = {
    // merge results at master
    val partialResults = partsRdd.collect()
    val master = new DistributedStep2Master(daalContext, classOf[lang.Double], args.getClusteringMethod, inputCentroids.numRows)

    for (value <- partialResults) {
      value.unpack(daalContext)
      master.input.add(DistributedStep2MasterInputId.partialResults, value)
    }
    master.compute
    master.finalizeCompute
  }
}
