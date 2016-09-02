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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import breeze.linalg._
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.scoring.models.ARIMAXData

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAXJsonProtocol._

@PluginDoc(oneLine = "New frame with column of predicted y values",
  extended = """Predict the time series values for a test frame, based on the specified
x values.  Creates a new frame revision with the existing columns and a new predicted_y
column.""",
  returns = """A new frame containing the original frame's columns and a column *predictied_y*""")
class ARIMAXPredictPlugin extends SparkCommandPlugin[ARIMAXPredictArgs, FrameReference] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:arimax/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ARIMAXPredictArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ARIMAXPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Extracting the ARIMAXModel from the stored JsObject
    val arimaxData = model.data.convertTo[ARIMAXData]
    val arimaxModel = arimaxData.arimaxModel

    require(arimaxData.xColumns.length == arguments.xColumns.length, "Number of columns for train and predict should be the same")

    // Get vector or y values and matrix of x values
    val (yVector, xMatrix) = ARXFunctions.getYandXFromFrame(frame.rdd, arguments.timeseriesColumn, arguments.xColumns)
    val ts = new DenseVector[Double](yVector.toArray)

    val predictions = arimaxModel.predict(ts, xMatrix).toArray

    if (predictions.length != frame.rdd.count)
      throw new RuntimeException("Received unexpected number of y values from ARIMAX Model predict (expected " +
        frame.rdd.count.toString + " values, but received " + predictions.length.toString + " values).")

    val dataWithPredictions = frame.rdd.zipWithIndex().map {
      case (row: Row, index: Long) =>
        Row.fromSeq(row.toSeq :+ predictions(index.toInt))
    }

    val schemaWithPredictions = frame.rdd.frameSchema.addColumn(Column("predicted_y", DataTypes.float64))
    val predictFrame = new FrameRdd(schemaWithPredictions, dataWithPredictions)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by ARIMAXModel predict command"))) {
      newFrame => newFrame.save(predictFrame)
    }
  }
}
