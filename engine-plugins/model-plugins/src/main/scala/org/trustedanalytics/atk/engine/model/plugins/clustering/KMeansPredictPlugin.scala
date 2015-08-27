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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, Naming }
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
import org.apache.spark.mllib.atk.plugins.VectorUtils._

import scala.collection.mutable.ListBuffer

@PluginDoc(oneLine = "Predict the cluster assignments for the data points.",
  extended = "",
  returns = """Frame
    A new frame consisting of the existing columns of the frame and new columns.
    The data returned is composed of multiple components:
'k' columns : double
    Containing squared distance of each point to every cluster center.
predicted_cluster : int
    Integer containing the cluster assignment.""")
class KMeansPredictPlugin extends SparkCommandPlugin[KMeansPredictArgs, FrameReference] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: KMeansPredictArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: KMeansPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Extracting the KMeansModel from the stored JsObject
    val kmeansData = model.data.convertTo[KMeansData]
    val kmeansModel = kmeansData.kMeansModel
    if (arguments.observationColumns.isDefined) {
      require(kmeansData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val kmeansColumns = arguments.observationColumns.getOrElse(kmeansData.observationColumns)
    val scalingValues = kmeansData.columnScalings

    //Predicting the cluster for each row
    val predictionsRDD = frame.rdd.mapRows(row => {
      val columnsArray = row.valuesAsArray(kmeansColumns).map(row => DataTypes.toDouble(row))
      val columnScalingsArray = scalingValues.toArray
      val doubles = columnsArray.zip(columnScalingsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)

      val clusterCenters = kmeansModel.clusterCenters

      for (i <- clusterCenters.indices) {
        val distance = toMahoutVector(point).getDistanceSquared(toMahoutVector(clusterCenters(i)))
        row.addValue(distance)
      }
      val prediction = kmeansModel.predict(point)
      row.addValue(prediction + 1)
    })

    //Updating the frame schema
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    for (i <- 1 to kmeansModel.clusterCenters.length) {
      val colName = "distance_from_cluster_" + i.toString
      columnNames += colName
      columnTypes += DataTypes.float64
    }
    columnNames += "predicted_cluster"
    columnTypes += DataTypes.int32

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val predictFrameRdd = new FrameRdd(updatedSchema, predictionsRDD)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by KMeans predict operation"))) { newPredictedFrame: FrameEntity =>
      newPredictedFrame.save(predictFrameRdd)
    }
  }

}
