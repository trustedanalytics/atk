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

import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublishArgs, ModelPublish }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, ApiMaturityTag, PluginDoc, CommandPlugin }
import org.trustedanalytics.atk.scoring.models.ARIMAData
import com.google.common.base.Charsets

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.scoring.ModelPublishJsonProtocol._
import org.trustedanalytics.atk.domain.datacatalog.DataCatalogRestResponseJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAJsonProtocol._

@PluginDoc(oneLine = "Creates a tar file that will be used as input to the scoring engine",
  extended = """The publish method exports the ARIMA Model and its implementation into a tar
              file.  The tar file is then published on HDFS and this method returns the path to
              to the tar file.  The tar file serves as input to the scoring engine.
              This model can then be used to predict future values.""",
  returns = """Returns the HDFS path to the trained model's tar file.""")
class ARIMAPublishPlugin extends CommandPlugin[ModelPublishArgs, ExportMetadata] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g. Python client vs code generation.
   */
  override def name: String = "model:arima/publish"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Get predicted future values, based on the values observed during training.
   *
   * @param arguments user supplied arguments to run this plugin.
   * @param invocation information about the user and circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelPublishArgs)(implicit invocation: Invocation): ExportMetadata = {
    val model: Model = arguments.model

    // Extracting ARIMAData from the stored JsObject
    val arimaData = model.data.convertTo[ARIMAData]
    val jsValue: JsValue = arimaData.toJson

    val modelArtifacts = ModelPublish.createTarForScoringEngine(jsValue.toString.getBytes(Charsets.UTF_8), "scoring-models", "org.trustedanalytics.atk.scoring.models.ARIMAModelReaderPlugin")

    ExportMetadata(modelArtifacts.filePath, "model", "tar", modelArtifacts.fileSize, model.name.getOrElse("arima_model"))
  }

}
