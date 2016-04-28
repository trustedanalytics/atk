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
package org.trustedanalytics.atk.engine.daal.plugins.dimensionalityreduction

import com.intel.daal.algorithms.svd._
import com.intel.daal.data_management.data.{ HomogenNumericTable, DataCollection }
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.daal.plugins.DistributedAlgorithm
import org.trustedanalytics.atk.engine.daal.plugins.tables.DaalConversionImplicits._
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ IndexedNumericTable, DistributedNumericTable }

case class SvdPartialResults(tableIndex: Long, vMatrixDataCollection: DataCollection, uMatrix: DataCollection)

case class SvdMasterResult(vMatrixMasterResult: Result, uMatrixMasterResult: DistributedStep2MasterPartialResult)

/**
 * Run Intel DAAL Principal Components Algorithm (PCA) using Singular Value Decomposition (SVD)
 *
 * @param frameRdd Input frame
 * @param args PCA arguments
 * @return PCA results with eigen values and vectors
 */
case class DaalSvdAlgorithm(frameRdd: FrameRdd,
                            args: DaalPrincipalComponentsTrainArgs,
                            computeU: Boolean = false) extends DistributedAlgorithm[SvdPartialResults, SvdMasterResult] {

  val distributedTable = DistributedNumericTable.createTable(createVectorRdd(args.meanCentered))

  def compute(): DaalSvdData = {
    val context = new DaalContext
    val partialResults = computePartialResults()

    val columnStatistics = frameRdd.columnStatistics(args.observationColumns)
    val k = args.k.getOrElse(args.observationColumns.length)
    val results = mergePartialResults(context, partialResults)
    val singularValues: Array[Double] = getSingularValues(results)
    val rightSingularMatrix: Array[Array[Double]] = getRightSingularMatrix(results, k)
    //val leftSingularMatrix = computeLeftSingularMatrix(results.uMatrixMasterResult, partialResults)

    DaalSvdData(
      k,
      args.observationColumns,
      args.meanCentered,
      columnStatistics.mean.toArray,
      singularValues,
      rightSingularMatrix
    )
  }

  def getRightSingularMatrix(results: SvdMasterResult, k: Int): Array[Array[Double]] = {
    val rightSingularMatrix = results.vMatrixMasterResult.get(
      ResultId.rightSingularMatrix).toTransposedArrayOfDoubleArray()
    val numCols = rightSingularMatrix(0).length
    if (k < numCols)
      rightSingularMatrix
    else
      rightSingularMatrix
  }

  def getSingularValues(results: SvdMasterResult): Array[Double] = {
    val singularValues = results.vMatrixMasterResult.get(ResultId.singularValues).toDoubleArray()
    singularValues
  }

  /**
   * Compute partial results for computing singular values, right and left singular matrices
   *
   * @return RDD of partial results with
   */
  override def computePartialResults(): RDD[SvdPartialResults] = {
    distributedTable.rdd.map(table => {
      val context = new DaalContext
      val svdLocal = new DistributedStep1Local(context, classOf[java.lang.Double], Method.defaultDense)
      svdLocal.input.set(InputId.data, table.getUnpackedTable(context))

      val partialResult = svdLocal.compute
      val index = table.index
      val vMatrixDataCollection = partialResult.get(PartialResultId.outputOfStep1ForStep2)
      val uMatrixDataCollection = partialResult.get(PartialResultId.outputOfStep1ForStep3)
      vMatrixDataCollection.pack()
      uMatrixDataCollection.pack()

      context.dispose()
      SvdPartialResults(index, vMatrixDataCollection, uMatrixDataCollection)
    })
  }

  /**
   * Merge partial PCA results and compute eigen values and vectors
   *
   * @param context DAAL context
   * @param rdd RDD of partial PCA results
   * @return PCA results with eigen values and vectors
   */
  override def mergePartialResults(context: DaalContext,
                                   rdd: RDD[SvdPartialResults]): SvdMasterResult = {
    val parts_List = rdd.map(p => (p.tableIndex, p.vMatrixDataCollection)).collect()
    val svdMaster = new DistributedStep2Master(context, classOf[java.lang.Double], Method.defaultDense)

    parts_List.foreach {
      case ((i, dataCollection)) =>
        dataCollection.unpack(context)
        svdMaster.input.add(DistributedStep2MasterInputId.inputOfStep2FromStep1, i.toInt, dataCollection)
    }

    val uMatrixMasterResult = svdMaster.compute
    val vMatrixMasterResult = svdMaster.finalizeCompute
    SvdMasterResult(vMatrixMasterResult, uMatrixMasterResult)
  }

  /**
   *
   * @param uMatrixMasterResult
   * @param rdd
   * @return
   */
  def computeLeftSingularMatrix(uMatrixMasterResult: DistributedStep2MasterPartialResult,
                                rdd: RDD[SvdPartialResults]): Option[RDD[Vector]] = {

    if (!computeU) return None

    val masterResultBcast = rdd.sparkContext.broadcast(uMatrixMasterResult)

    val leftSingularMatrixRdd = rdd.map(row => {
      val context = new DaalContext()
      val masterResult = masterResultBcast.value
      masterResult.unpack(context)
      val uMatrixInput = masterResult.get(DistributedPartialResultCollectionId.outputOfStep2ForStep3)
      val uMatrixLocalInput = uMatrixInput.get(row.tableIndex.toInt)
      val uMatrixDataCollection = row.uMatrix
      uMatrixDataCollection.unpack(context)
      val svdLocal = new DistributedStep3Local(context, classOf[java.lang.Double], Method.defaultDense)
      svdLocal.input.set(DistributedStep3LocalInputId.inputOfStep3FromStep1, uMatrixDataCollection)
      //svdLocal.input.set(DistributedStep3LocalInputId.inputOfStep3FromStep2, uMatrixLocalInput)
      svdLocal.compute()
      val result = svdLocal.finalizeCompute()

      val uMatrix = result.get(ResultId.leftSingularMatrix).asInstanceOf[HomogenNumericTable]
      val leftSingularMatrix = IndexedNumericTable(row.tableIndex, uMatrix)
      context.dispose()
      (leftSingularMatrix, leftSingularMatrix.numRows)
    })

    val totalRows = leftSingularMatrixRdd.map { case (t, n) => n }.reduce(_ + _)
    val table = new DistributedNumericTable(leftSingularMatrixRdd.keys, totalRows)
    Some(table.toVectorRdd())
  }

  /**
   * Create Vector RDD from observation columns in frame
   *
   * @param meanCentered If true, return a mean-centered vector RDD
   * @return Vector RDD
   */
  private def createVectorRdd(meanCentered: Boolean): RDD[Vector] = {
    val vectorRdd = meanCentered match {
      case true => frameRdd.toMeanCenteredDenseVectorRDD(args.observationColumns)
      case false => frameRdd.toDenseVectorRDD(args.observationColumns)
    }
    vectorRdd
  }
}
