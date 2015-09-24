//////////////////////////////////////////////////////////////////////////////
// INTEL CONFIDENTIAL
//
// Copyright 2015 Intel Corporation All Rights Reserved.
//
// The source code contained or described herein and all documents related to
// the source code (Material) are owned by Intel Corporation or its suppliers
// or licensors. Title to the Material remains with Intel Corporation or its
// suppliers and licensors. The Material may contain trade secrets and
// proprietary and confidential information of Intel Corporation and its
// suppliers and licensors, and is protected by worldwide copyright and trade
// secret laws and treaty provisions. No part of the Material may be used,
// copied, reproduced, modified, published, uploaded, posted, transmitted,
// distributed, or disclosed in any way without Intel's prior express written
// permission.
//
// No license under any patent, copyright, trade secret or other intellectual
// property right is granted to or conferred upon you by disclosure or
// delivery of the Materials, either expressly, by implication, inducement,
// estoppel or otherwise. Any license under such intellectual property rights
// must be express and approved by Intel in writing.
//////////////////////////////////////////////////////////////////////////////

package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, PluginDoc, ApiMaturityTag, Invocation }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.regression.LabeledPoint
import ModelPluginImplicits._
import org.apache.spark.rdd.RDD

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class NaiveBayesTrainArgs(model: ModelReference,
                               @ArgDoc("""A frame to train the model on.""") frame: FrameReference,
                               @ArgDoc("""Column containing the label for each
observation.""") labelColumn: String,
                               @ArgDoc("""Column(s) containing the
observations.""") observationColumns: List[String],
                               @ArgDoc("""Additive smoothing parameter
Default is 1.0.""") lambdaParameter: Option[Double] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
}

@PluginDoc(oneLine = "Build a naive bayes model.",
  extended = """Train a NaiveBayesModel using the observation column, label column of the train frame and an optional lambda value.""")
class NaiveBayesTrainPlugin extends SparkCommandPlugin[NaiveBayesTrainArgs, UnitReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: NaiveBayesTrainArgs)(implicit invocation: Invocation) = 109

  /**
   * Run MLLib's NaiveBayes() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: NaiveBayesTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = frame.rdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)

    //Running MLLib
    val naiveBayes = new NaiveBayes()
    naiveBayes.setLambda(arguments.lambdaParameter.getOrElse(1.0))

    val naiveBayesModel = naiveBayes.run(labeledTrainRdd)
    val jsonModel = new NaiveBayesData(naiveBayesModel, arguments.observationColumns)

    model.data = jsonModel.toJson.asJsObject
  }
}
