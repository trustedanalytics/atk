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

package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublishJsonProtocol, ModelPublish, ModelPublishArgs }
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.scoring.models.DaalLinearRegressionReaderPlugin
import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata
import com.google.common.base.Charsets

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.datacatalog.DataCatalogRestResponseJsonProtocol._
import DaalLinearRegressionModelFormat._
import ModelPublishJsonProtocol._

/**
 * Publish an Intel DAAL linear regression model for scoring
 */
@PluginDoc(oneLine = "Creates a tar file that will be used as input to the scoring engine",
  extended =
    """The publish method exports the Intel DAAL linear regression model
       and its implementation into a tar file. The tar file is then published
       on HDFS and this method returns the path to the tar file.
       The tar file serves as input to the scoring engine.
       This model can then be used to predict the target value of an observation.""",
  returns = """Returns the HDFS path to the trained model's tar file""")
class DaalLinearRegressionPublishPlugin extends CommandPlugin[ModelPublishArgs, ExportMetadata] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_linear_regression/publish"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelPublishArgs)(implicit invocation: Invocation): ExportMetadata = {
    DaalUtils.validateDaalLibraries(EngineConfig.daalDynamicLibraries)
    val model: Model = arguments.model

    //Extracting the DaalLinearRegressionModel from the stored JsObject
    val lrJsObject = model.data
    val trainedModel = lrJsObject.convertTo[DaalLinearRegressionModelData]

    val modelartifacts = ModelPublish.createTarForScoringEngine(lrJsObject.toString().getBytes(Charsets.UTF_8),
      "scoring-models", classOf[DaalLinearRegressionReaderPlugin].getName,
      Some(DaalUtils.getDaalLibraryPaths(EngineConfig.daalDynamicLibraries)))

    ExportMetadata(modelartifacts.filePath, "model", "tar", modelartifacts.fileSize, model.name.getOrElse("daal_linear_regression"))
  }
}
