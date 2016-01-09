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

package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import com.intel.daal.algorithms.ModelSerializer
import com.intel.daal.services.DaalContext
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, SparkCommandPlugin, ApiMaturityTag, Invocation }

//Implicits needed for JSON conversion
import spray.json._
import DaalLinearRegressionModelDataFormat._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

import DaalLinearRegressionJsonFormat._

/**
 * Plugin for scoring DAAL's Linear Regression using QR decomposition
 */
@PluginDoc(oneLine = "Make new frame with column for label prediction.",
  extended = """Predict the labels for a test frame and create a new frame revision with
existing columns and a new predicted value column.""",
  returns =
    """frame\:
  Frame containing the original frame's columns and a column with the predicted value.""")
class DaalLinearRegressionPredictPlugin extends SparkCommandPlugin[DaalLinearRegressionArgs, FrameReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /** Disable Kryo serialization to prevent seg-faults when using DAAL */
  override def kryoRegistrator: Option[String] = None

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DaalLinearRegressionArgs)(implicit invocation: Invocation) = 2

  /**
   * Get predictions for DAAL's Linear Regression with QR decomposition using test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalLinearRegressionArgs)(implicit invocation: Invocation): FrameReference =
    {
      DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)
      val model: Model = arguments.model

      //create RDD from the frame
      val testFrame: SparkFrame = arguments.frame
      val featureColumns = arguments.featureColumns
      val labelColumns = arguments.labelColumns

      //Load the libsvm model
      val lrJsObject = model.data
      val modelData = lrJsObject.convertTo[DaalLinearRegressionModelData]

      require(modelData.featureColumns.length == arguments.featureColumns.length,
        "Number of feature columns for train and predict should be same")
      require(modelData.labelColumns.length == arguments.labelColumns.length,
        "Number of label columns for train and predict should be same")

      val lrResultsFrameRdd = DaalLinearRegressionFunctions.predictLinearModel(
        modelData,
        testFrame.rdd,
        featureColumns)

      engine.frames.tryNewFrame(CreateEntityArgs(
        description = Some("created by DAAL linear regression predict operation"))) {
        newPredictedFrame: FrameEntity =>
          newPredictedFrame.save(lrResultsFrameRdd)
      }
    }

}
