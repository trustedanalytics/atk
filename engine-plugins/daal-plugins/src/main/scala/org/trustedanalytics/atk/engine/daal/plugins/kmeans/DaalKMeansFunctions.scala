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
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }

object DaalKMeansFunctions extends Serializable {

  /**
   * Train K-means clustering model
   *
   * @param frameRdd Input frame
   * @param args Input arguments
   * @return Trained k-means model
   */
  def trainKMeansModel(frameRdd: FrameRdd,
                       args: DaalKMeansTrainArgs): DaalKMeansResults = {
    val table = DistributedNumericTable.createTable(frameRdd, args.observationColumns)
    table.cache()

    // Iteratively update cluster centroids
    var centroids = DaalCentroidsInitializer(table, args).initializeCentroids()
    for (i <- 1 until args.maxIterations) {
      val result = DaalCentroidsUpdater(table, centroids, args.labelColumn).updateCentroids()
      centroids = result.centroids
    }

    // Run final iteration and get cluster assignments
    val finalResults = DaalCentroidsUpdater(table, centroids, args.labelColumn, assignFlag = true).updateCentroids()
    centroids = finalResults.centroids
    table.unpersist()

    // Create frame with cluster assignments
    val assignmentFrame = finalResults.getAssignmentFrame
    val kMeansResults = DaalKMeansResults(centroids, args.k, Some(frameRdd.zipFrameRdd(assignmentFrame)))
    kMeansResults
  }

  /**
   * Predict cluster assignments for KMeans model
   *
   * @param arguments Prediction input arguments
   * @param frame Input frame
   * @param modelData KMeans model data
   * @return Frame with cluster assignmemts
   */
  def predictKMeansModel(arguments: DaalKMeansPredictArgs, frame: FrameRdd, modelData: DaalKMeansModelData): FrameRdd = {
    val observationColumns = arguments.observationColumns.getOrElse(modelData.observationColumns)
    val labelColumn = arguments.labelColumn.getOrElse(modelData.labelColumn)

    // Compute cluster assignments
    val table = DistributedNumericTable.createTable(frame, observationColumns)
    val centroids = IndexedNumericTable.createTable(modelData.centroids)
    val finalResults = DaalCentroidsUpdater(table, centroids, labelColumn, assignFlag = true).updateCentroids()

    // Create assignment frame
    val assignmentFrame = frame.zipFrameRdd(finalResults.getAssignmentFrame)
    assignmentFrame
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

}
