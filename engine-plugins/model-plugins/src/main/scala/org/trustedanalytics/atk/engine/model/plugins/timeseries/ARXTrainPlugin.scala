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
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.linalg.Matrices
import breeze.linalg._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
import com.cloudera.sparkts.{ ARXModel, AutoregressionX, Autoregression }
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARXJsonProtocol._
import scala.collection.mutable.ArrayBuffer

@PluginDoc(oneLine = "Creates AutoregressionX (ARX) Model from train frame.",
  extended = "Creating a AutoregressionX (ARX) Model using the observation columns.",
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
    val model = arguments.model
    val trainFrameRdd = frame.rdd

    trainFrameRdd.cache()

    var arxModels = List[ARXModel]()
    val (yVector, xMatrix) = ARXFunctions.getYandXFromFrame(trainFrameRdd, arguments.timeseriesColumn, arguments.xColumns)

    val arxModel = AutoregressionX.fitModel(yVector, xMatrix, arguments.yMaxLag, arguments.xMaxLag, true, arguments.noIntercept)
    val jsonModel = new ARXData(arxModel)
    model.data = jsonModel.toJson.asJsObject

    return ARXTrainReturn(arxModel.c, arxModel.coefficients)

    /*
    for (row <- trainFrameRdd.collect()) {
      val (yVector, xMatrix) = ARXFunctions.getYandXFromFrame(trainFrameRdd, trainFrameRdd.frameSchema, timeseriesColumn, xColumns)

      val arxModel = AutoregressionX.fitModel(yVector, xMatrix, arguments.yMaxLag, arguments.xMaxLang, true, arguments.noIntercept)
      val jsonModel = new ARXData(arxModel)
      model.data = jsonModel.toJson.asJsObject
      //val key = row.getString(trainFrameRdd.frameSchema.columnIndex(keyColumn))
      //arxModels ::= arxModel
      //keys ::= key
    }*/

    //val jsonModel = new ARXData(keys, arxModels)
    //model.data = jsonModel.toJson.asJsObject
  }

}