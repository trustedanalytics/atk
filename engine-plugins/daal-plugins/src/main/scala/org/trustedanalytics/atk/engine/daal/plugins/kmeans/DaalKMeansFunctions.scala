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
import org.apache.spark.mllib.atk.plugins.VectorUtils._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column, DataTypes }
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }
import org.apache.mahout.math.{ DenseVector => MahoutDenseVector }
import org.trustedanalytics.atk.engine.daal.plugins.tables.DaalConversionImplicits._
import scala.collection.mutable.ListBuffer

object DaalKMeansFunctions extends Serializable {

  /**
   * Train K-means clustering model
   *
   * @param frameRdd Input frame
   * @param args Input arguments
   * @return Trained k-means model
   */
  def trainKMeansModel(frameRdd: FrameRdd,
                       args: DaalKMeansTrainArgs): DaalKMeansTrainReturn = {

    val vectorRdd = createVectorRdd(frameRdd, args.observationColumns, args.columnScalings)
    val table = DistributedNumericTable.createTable(vectorRdd)
    table.cache()

    // Iteratively update cluster centroids
    var centroids = DaalCentroidsInitializer(table, args).initializeCentroids()
    for (i <- 1 to args.maxIterations) {
      centroids = DaalCentroidsUpdater(table, centroids, args.labelColumn).updateCentroids()
    }

    // Create frame with cluster assignments
    val clusterAssigner = DaalClusterAssigner(table, centroids, args.labelColumn)
    val assignmentFrame = clusterAssigner.assign()
    val clusterSizes = clusterAssigner.clusterSizes(assignmentFrame)

    //Get dictionary with centroids
    val centroidsMap = centroids.table.toArrayOfDoubleArray().zipWithIndex.map {
      case (centroid, i) =>
        ("Cluster:" + i.toString, centroid)
    }.toMap
    table.unpersist()
    val kMeansResults = DaalKMeansTrainReturn(centroidsMap, clusterSizes)
    kMeansResults
  }

  /**
   * Predict cluster assignments for KMeans model
   *
   * @param args Prediction input arguments
   * @param frameRdd Input frame
   * @param modelData KMeans model data
   * @return Frame with cluster assignments
   */
  def predictKMeansModel(args: DaalKMeansPredictArgs, frameRdd: FrameRdd, modelData: DaalKMeansModelData): FrameRdd = {
    val observationColumns = args.observationColumns.getOrElse(modelData.observationColumns)
    val labelColumn = args.labelColumn.getOrElse(modelData.labelColumn)

    // Compute cluster assignments
    val vectorRdd = createVectorRdd(frameRdd, observationColumns, modelData.columnScalings)
    vectorRdd.cache()
    val table = DistributedNumericTable.createTable(vectorRdd)
    val centroids = IndexedNumericTable.createTable(0L, modelData.centroids)

    // Create assignment and cluster distances frame
    val assignFrame = DaalClusterAssigner(table, centroids, labelColumn).assign()
    val distanceFrame = computeClusterDistances(vectorRdd, modelData.centroids)
    frameRdd.zipFrameRdd(distanceFrame).zipFrameRdd(assignFrame)
  }

  /**
   * Compute distances to cluster centroids for each observation in Vector RDD
   *
   * @param vectorRdd Vector RDD
   * @param centroids Cluster centroids
   * @return Frame with 'k' columns with squared distance of each observation to 'k'th cluster center
   */
  def computeClusterDistances(vectorRdd: RDD[Vector], centroids: Array[Array[Double]]): FrameRdd = {
    val rowRdd: RDD[Row] = vectorRdd.map(vector => {
      val distances: Array[Any] = centroids.map(centroid => {
        toMahoutVector(vector).getDistanceSquared(new MahoutDenseVector(centroid))
      })
      new GenericRow(distances)
    })

    val columns = new ListBuffer[Column]()
    for (i <- 0 until centroids.length) {
      val colName = "distance_from_cluster_" + i.toString
      columns += Column(colName, DataTypes.float64)
    }
    new FrameRdd(FrameSchema(columns.toList), rowRdd)
  }

  /**
   * Get goal function for k-means
   *
   * The goal function is the sum of distances of observations to their closest cluster center
   *
   * @param result k-means result
   * @return sum of distances of observations to their closest cluster center
   */
  private def getGoalFunction(result: Result): Double = {
    //TODO: Goal function is returning zero in DAAL 2016.0.109. Revisit after upgrade
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
   * Create Vector RDD from observation columns in frame
   *
   * @param frameRdd Input frame
   * @param observationColumns Observation columns
   * @param columnScalings Optional column scalings for each of the observation columns
   * @return Vector RDD
   */
  private def createVectorRdd(frameRdd: FrameRdd,
                              observationColumns: List[String],
                              columnScalings: Option[List[Double]] = None): RDD[Vector] = {
    val vectorRdd = columnScalings match {
      case Some(scalings) => frameRdd.toDenseVectorRDDWithWeights(observationColumns, scalings)
      case _ => frameRdd.toDenseVectorRDD(observationColumns)
    }
    vectorRdd
  }

}
