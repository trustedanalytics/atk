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
package org.trustedanalytics.atk.engine.model.plugins.survivalanalysis

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ Invocation, ApiMaturityTag, SparkCommandPlugin }
import org.trustedanalytics.atk.scoring.models.LinearRegressionData

import scala.collection.mutable.ListBuffer

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.ml.atk.plugins.MLJsonProtocol._

class MultivariateCoxPredictPlugin extends SparkCommandPlugin[MultivariateCoxPredictArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:multivariate_cox/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Predict values for a frame using a trained Linear Regression model
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: MultivariateCoxPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame
    val predictFrameRdd = frame.rdd

    val coxJsObject = model.dataOption.getOrElse(throw new RuntimeException("This model has not be trained yet. Please train before trying to predict"))
    val coxData = coxJsObject.convertTo[MultivariateCoxData]
    val coxModel = coxData.coxModel
    val featureColumns = arguments.featureColumns.getOrElse(coxData.featureColumns)

    val hazardRatios = frame.rdd.mapRows(row => {
      val observation = row.valuesAsDenseVector(featureColumns)
      val hazardRatio = coxModel.predict(observation)
      row.addValue(hazardRatio)
    })
    val hazardRatioColumn = Column("hazard_ratio", DataTypes.float64)
    val predictFrame = frame.rdd.addColumn(hazardRatioColumn, row => {
      val observation = row.valuesAsDenseVector(featureColumns)
      coxModel.predict(observation)
    })

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by Multivariate Cox Proportional Hazards predict operation"))) {
      newPredictedFrame: FrameEntity =>
        newPredictedFrame.save(predictFrame)
    }

  }
}