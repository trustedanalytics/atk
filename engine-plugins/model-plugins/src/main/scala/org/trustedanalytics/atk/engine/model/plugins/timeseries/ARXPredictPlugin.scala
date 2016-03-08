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

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, Naming }
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.engine.frame.{ RowWrapper, SparkFrame }
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import breeze.linalg._
import org.trustedanalytics.atk.scoring.models.ARXData
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARXJsonProtocol._

import scala.collection.mutable.ListBuffer

@PluginDoc(oneLine = "New frame with column of predicted y values",
  extended = """Predict the time series values for a test frame, based on the specified
x values.  Creates a new frame revision with the existing columns and a new predicted_y
column.""",
  returns = """A new frame containing the original frame's columns and a column *predictied_y*""")
class ARXPredictPlugin extends SparkCommandPlugin[ARXPredictArgs, FrameReference] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:arx/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ARXPredictArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ARXPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //Extracting the ARXModel from the stored JsObject
    val arxData = model.data.convertTo[ARXData]
    val arxModel = arxData.arxModel

    require(arxData.xColumns.length == arguments.xColumns.length, "Number of columns for train and predict should be the same")

    // Add column of predicted y values
    val predictColumn = Column("predicted_y", DataTypes.float64)
    val predictFrame = frame.rdd.addColumn(predictColumn, row => {
      val yValue = row.doubleValue(arguments.timeseriesColumn)
      val xValues = row.valuesAsDoubleArray(arguments.xColumns)
      val predictedValues = arxModel.predict(new DenseVector(Array[Double](yValue)), new DenseMatrix(rows = 1, cols = xValues.length, data = xValues))
      if (predictedValues.length != 1)
        throw new RuntimeException("Unexpected number of predicted values returned from ARX (expected 1 value, received " + predictedValues.length.toString + ").")
      predictedValues(0)
    })

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by ARXModel predict command"))) {
      newFrame => newFrame.save(predictFrame)
    }
  }
}
