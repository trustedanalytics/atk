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

package org.trustedanalytics.atk.engine.model.plugins.clustering.lda

import org.trustedanalytics.atk.domain.StringValue
import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublishJsonProtocol, ModelPublish, ModelPublishArgs }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, CommandPlugin, Invocation, PluginDoc }
import com.google.common.base.Charsets
// Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import ModelPublishJsonProtocol._
import org.trustedanalytics.atk.domain.datacatalog.DataCatalogRestResponseJsonProtocol._

import spray.json._
import LdaJsonFormat._

/**
 *  Publish Latent Dirichlet Allocation model for scoring
 */
@PluginDoc(oneLine = "Creates a tar file that will used as input to the scoring engine",
  extended = """Creates a tar file with the trained Latent Dirichlet Allocation model. The tar file is then published on HDFS and this method returns the path to the tar file.
              The tar file is used as input to the scoring engine to predict the conditional topic probabilities for a document.""",
  returns = """Returns the HDFS path to the trained model's tar file""")
class LdaPublishPlugin extends CommandPlugin[ModelPublishArgs, ExportMetadata] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/publish"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: ModelPublishArgs)(implicit invocation: Invocation) = 1

  /**
   * Publish trained Latent Dirichlet Allocation model
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ModelPublishArgs)(implicit invocation: Invocation): ExportMetadata = {

    val model: Model = arguments.model

    val modelArtifact = ModelPublish.createTarForScoringEngine(model.data.toString().getBytes(Charsets.UTF_8),
      "scoring-models",
      "org.trustedanalytics.atk.scoring.models.LdaModelReaderPlugin")
    ExportMetadata(modelArtifact.filePath, "model", "tar", modelArtifact.fileSize, model.name.getOrElse("lda_model"))
  }
}
