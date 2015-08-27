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
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.FrameRddImplicits._
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, ApiMaturityTag, Invocation, SparkCommandPlugin }
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

//Implicits needed for JSON conversion
import spray.json._

@PluginDoc(oneLine = "Build Random Forests Classifier model.",
  extended = """Creating a Random Forests Classifier Model using the observation columns and label column.""",
  returns =
    """Values of the Random Forest Classifier model object storing:
      | the list of observation columns on which the model was trained,
      | the column name containing the labels of the observations,
      | the number of classes,
      | the number of decison trees in the random forest,
      | the number of nodes in the random forest,
      | the map storing arity of categorical features,
      | the criterion used for information gain calculation,
      | the maximum depth of the tree,
      | the maximum number of bins used for splitting features,
      | the random seed used for bootstrapping and choosing feature subset.
    """.stripMargin)
class RandomForestClassifierTrainPlugin extends SparkCommandPlugin[RandomForestClassifierTrainArgs, RandomForestClassifierTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_classifier/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: RandomForestClassifierTrainArgs)(implicit invocation: Invocation) = 109

  /**
   * Run MLLib's NaiveBayes() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RandomForestClassifierTrainArgs)(implicit invocation: Invocation): RandomForestClassifierTrainReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = frame.rdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)
    val randomForestModel = RandomForest.trainClassifier(labeledTrainRdd, arguments.numClasses, arguments.getCategoricalFeaturesInfo, arguments.numTrees,
      arguments.getFeatureSubsetCategory, arguments.impurity, arguments.maxDepth, arguments.maxBins, arguments.seed)
    val jsonModel = new RandomForestClassifierData(randomForestModel, arguments.observationColumns, arguments.numClasses)

    model.data = jsonModel.toJson.asJsObject
    new RandomForestClassifierTrainReturn(arguments.observationColumns, arguments.labelColumn, arguments.numClasses,
      randomForestModel.numTrees, randomForestModel.totalNumNodes, arguments.getFeatureSubsetCategory, arguments.impurity,
      arguments.maxDepth, arguments.maxBins, arguments.seed)
  }
}
