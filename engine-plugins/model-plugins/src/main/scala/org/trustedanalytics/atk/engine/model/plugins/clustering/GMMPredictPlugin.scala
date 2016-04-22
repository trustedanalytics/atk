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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, Naming }
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import scala.collection.mutable.ListBuffer
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.linalg.Vectors

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Predict the cluster assignments for the data points.",
  extended = "Predicts the clusters for each data point of the frame using the trained model",
  returns = """Frame
    A new frame consisting of the existing columns of the frame and a new column:
predicted_cluster : int
    Integer containing the cluster assignment.""")
class GMMPredictPlugin extends SparkCommandPlugin[GMMPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:gmm/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GMMPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    require(!frame.rdd.isEmpty(), "Predict Frame is empty. Please predict on a non-empty Frame.")
    //Extracting the GmmModel from the stored JsObject
    val gmmData = model.data.convertTo[GMMData]
    val gmmModel = gmmData.gmmModel
    if (arguments.observationColumns.isDefined) {
      require(gmmData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }

    val gmmColumns = arguments.observationColumns.getOrElse(gmmData.observationColumns)
    val scalingValues = gmmData.columnScalings

    val predictionsRdd = gmmModel.predict(frame.rdd.toDenseVectorRDDWithWeights(gmmColumns, scalingValues))
    val indexedPredictionsRdd = predictionsRdd.zipWithIndex().map { case (cluster, index) => (index, cluster) }
    val indexedFrameRdd = frame.rdd.zipWithIndex().map { case (row, index) => (index, row) }

    val resultRdd: RDD[Row] = indexedPredictionsRdd.join(indexedFrameRdd).map { value =>
      val row = value._2._2
      val cluster = value._2._1
      new GenericRow(row.toSeq.toArray :+ cluster)
    }

    //Updating the frame schema
    var columnNames = new ListBuffer[String]()
    var columnTypes = new ListBuffer[DataTypes.DataType]()
    columnNames += "predicted_cluster"
    columnTypes += DataTypes.int32

    val newColumns = columnNames.toList.zip(columnTypes.toList.map(x => x: DataType))
    val updatedSchema = frame.schema.addColumns(newColumns.map { case (name, dataType) => Column(name, dataType) })
    val predictFrameRdd = new FrameRdd(updatedSchema, resultRdd)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by GMM predict operation"))) { newPredictedFrame: FrameEntity =>
      newPredictedFrame.save(predictFrameRdd)
    }
  }

}
