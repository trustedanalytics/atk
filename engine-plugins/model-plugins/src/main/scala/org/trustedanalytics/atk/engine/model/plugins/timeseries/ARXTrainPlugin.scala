/**
 *  Copyright (c) 2016 Intel Corporation 
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

//Implicits needed for JSON conversion

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.linalg.Matrices
import breeze.linalg._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
import com.cloudera.sparkts.{ ARXModel, AutoregressionX }
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARXJsonProtocol._
import scala.collection.mutable.WrappedArray

@PluginDoc(oneLine = "Creates AutoregressionX (ARX) Model from train frame.",
  extended = "Creating a AutoregressionX (ARX) Model Model using the observation columns.",
  returns = """dictionary
    A dictionary with trained ARX model with the following keys\:
'cluster_size' : dictionary with 'Cluster:id' as the key and the corresponding cluster size is the value
'within_set_sum_of_squared_error' : The set of sum of squared error for the model.""")
class ARXTrainPlugin extends SparkCommandPlugin[ARXTrainArgs, ARXTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:arx/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  /**
   * Number of Spark jobs that get created by running this command
   *
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ARXTrainArgs)(implicit invocation: Invocation) = 15
  /**
   * Run the spark time series ARX fitmodel() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   * as well as a function that can be called to produce a SparkContext that
   * can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ARXTrainArgs)(implicit invocation: Invocation): ARXTrainReturn = {
    val frame: SparkFrame = arguments.frame

    val trainFrameRdd = frame.rdd
    val timeseriesColumn = arguments.timeseriesColumn
    val xColumns = arguments.xColumns
    var timeseriesLength: Long = 0 // vector length for timeseries column

    warn("ARX Train Plugin:  IN")
    timeseriesLength = ARXTrainPlugin.verifyVectorColumn(trainFrameRdd, timeseriesColumn, "timeseriesColumn")

    for (xColumn <- xColumns) {
      val xColumnVectorLength = ARXTrainPlugin.verifyVectorColumn(trainFrameRdd, xColumn, "xColumns")

      if (xColumnVectorLength != timeseriesLength)
        throw new IllegalArgumentException(s"xColumn named $xColumn vector must be the same length as the time series vector.")
    }

    trainFrameRdd.cache()

    val timeseriesColIndex = trainFrameRdd.frameSchema.columnIndex(timeseriesColumn)
    var str: String = "timeseries lengths (column index: " + timeseriesColIndex.toString + ", count: " + trainFrameRdd.count().toString + "): "

    str += "... \n"

    str += "xColumns (" + xColumns.size.toString + ") : "

    for (xCol <- xColumns) {
      str += xCol + ", "
    }

    str += "\n"

    for (row <- trainFrameRdd.collect()) {
      val tsVector = new DenseVector(row.get(timeseriesColIndex).asInstanceOf[WrappedArray[Double]].toArray)
      val xMatrix = new DenseMatrix(rows = xColumns.size, cols = timeseriesLength.toInt)

      var colIndex = 0
      var rowIndex = 0

      for (rowIndex <- 0 until xColumns.size) {
        val xSchemaIndex = trainFrameRdd.frameSchema.columnIndex(xColumns(rowIndex))
        str += "\n" + xColumns(rowIndex).toString + " column Index: " + xSchemaIndex.toString + ", "

        for (colIndex <- 0 until timeseriesLength.toInt) {
          str += row.getString(colIndex).toString + ", "
          //xMatrix.(rowIndex, colIndex) = row.getDouble(colIndex)
        }

        str += "\n"
      }
    }

    val testc = 5.0
    val testcoefficients = new Array[Double](1)

    throw new RuntimeException(str)

    ARXTrainReturn(testc, testcoefficients)

    /*
    val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(arguments.observationColumns, arguments.columnScalings)
    val kmeansModel = kMeans.run(vectorRDD)
    val size = KMeansTrainPlugin.computeClusterSize(kmeansModel, trainFrameRdd, arguments.observationColumns, arguments.columnScalings)
    val withinSetSumOfSquaredError = kmeansModel.computeCost(vectorRDD)
    trainFrameRdd.unpersist()

    //Writing the kmeansModel as JSON
    val jsonModel = new KMeansData(kmeansModel, arguments.observationColumns, arguments.columnScalings)
    val model: Model = arguments.model
    model.data = jsonModel.toJson.asJsObject

    KMeansTrainReturn(size, withinSetSumOfSquaredError)*/
  }

  def arxTrainFormatter(frameRdd: FrameRdd, timeseriesColumn: String, xColumn: String): Unit = {
    frameRdd.map(row => columnFormatterForTrain(row.toSeq.toArray.zipWithIndex)).collect()

    for (row <- frameRdd.collect()) {
      val rowSeq = row.toSeq
    }
  }

  private def columnFormatterForTrain(valueIndexPairArray: Array[(Any, Int)]): String = {
    val result = for {
      i <- valueIndexPairArray
      value = i._1
      index = i._2
      if index != 0 && value != 0
    } yield s"$index:$value"
    s"${valueIndexPairArray(0)._1} ${result.mkString(" ")}"
  }
}

object ARXTrainPlugin {
  /**
   * Checks the frame's schema to verify that the specified column exists and is a vector.
   * @param frameRdd  Frame to check
   * @param columnName  Name of the vector column to check
   * @param parameterName  Name of the column parameter - just used in the exception message to
   *                       make it more clear to the user which parameter is invalid (i.e. "timeseriesColumn")
   * @return The length of the specified vector column
   */
  def verifyVectorColumn(frameRdd: FrameRdd, columnName: String, parameterName: String): Long = {
    var vectorLength: Long = 0

    if (frameRdd.frameSchema.hasColumn(columnName) == false) {
      // Column does not exist
      throw new IllegalArgumentException(s"$parameterName column named $columnName does not exist in the frame's schema.")
    }
    else {
      // Verify that it's a vector column
      frameRdd.frameSchema.columnDataType(columnName) match {
        case DataTypes.vector(length) => vectorLength = length
        case _ => throw new IllegalArgumentException(s"$parameterName column $columnName must be a vector.")
      }
    }

    return vectorLength
  }
}