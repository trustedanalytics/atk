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

package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import com.intel.daal.algorithms.ModelSerializer
import com.intel.daal.services.DaalContext
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.{ ArgDocAnnotation, PluginDocAnnotation, EngineConfig }
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.daal.plugins.conversions.DaalConversionImplicits
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._
import DaalConversionImplicits._

import scala.util.{ Success, Failure, Try }

/** Json conversion for arguments and return value case classes */
object DaalLinearRegressionJsonFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val lrTrainFormat = jsonFormat4(DaalLinearRegressionArgs)
  implicit val lrTrainResultFormat = jsonFormat1(DaalLinearRegressionTrainResult)
}

/**
 * Arguments for training and scoring DAAL linear regression model
 *
 * @param model Handle to the model to be written to.
 * @param frame Handle to the data frame
 * @param featureColumns Handle to the observation column of the data frame
 */
case class DaalLinearRegressionArgs(model: ModelReference,
                                    @ArgDoc("""A frame to train or test the model on.""") frame: FrameReference,
                                    @ArgDoc("""List of column(s) containing the
observations.""") featureColumns: List[String],
                                    @ArgDoc("""Column name containing the label
for each observation.""") labelColumns: List[String]) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(featureColumns != null && !featureColumns.isEmpty, "observationColumn must not be null nor empty")
  require(labelColumns != null && !labelColumns.isEmpty, "labelColumn must not be null nor empty")
}

/**
 * Results of training DAAL linear regression model
 *
 * @param betas Beta parameters for trained linear regression model
 */
case class DaalLinearRegressionTrainResult(betas: Array[Array[Double]])

import spray.json._
import DaalLinearRegressionModelFormat._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalLinearRegressionJsonFormat._

/** Plugin for training DAAL's Linear Regression using QR decomposition */
@PluginDoc(oneLine = "Build DAAL linear regression model.",
  extended = "Create DAAL LinearRegression Model using the observation column and target column of the train frame",
  returns = "Array with coefficients of linear regression model")
class DaalLinearRegressionTrainPlugin extends SparkCommandPlugin[DaalLinearRegressionArgs, DaalLinearRegressionTrainResult] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /** Disable Kryo serialization to prevent seg-faults when using DAAL */
  override def kryoRegistrator: Option[String] = None

  /**
   * Run DAAL's Linear Regression with QR decomposition on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DaalLinearRegressionArgs)(implicit invocation: Invocation): DaalLinearRegressionTrainResult =
    {
      DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)
      val model: Model = arguments.model

      // Create RDD from the frame
      val trainFrame: SparkFrame = arguments.frame
      val featureColumns = arguments.featureColumns
      val labelColumns = arguments.labelColumns

      // Train model
      val context = new DaalContext()
      val trainModel = DaalLinearRegressionFunctions.trainLinearModel(
        context,
        trainFrame.rdd,
        featureColumns,
        labelColumns)
      val betas = trainModel.getBeta()
      val betaArray = betas.toArrayOfDoubleArray()

      // Save model results to metastore
      val serializedModel = Try(ModelSerializer.serializeQrModel(trainModel).toList) match {
        case Success(sModel) => sModel
        case Failure(ex) => {
          println(s"Unable to serialize DAAL model : ${ex.getMessage}")
          throw new RuntimeException(s"Unable to serialize DAAL model : ${ex.getMessage}")
        }
      }

      val jsonModel = DaalLinearRegressionModel(serializedModel,
        featureColumns,
        labelColumns).toJson.asJsObject
      model.data = jsonModel

      // Dispose DAAL data structures
      context.dispose()
      // Return trained model
      DaalLinearRegressionTrainResult(betaArray)
    }

}
