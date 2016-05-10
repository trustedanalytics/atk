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

package org.trustedanalytics.atk.engine.daal.plugins.classification.naivebayes

import com.google.common.base.Charsets
import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.daal.plugins.DaalUtils
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublishJsonProtocol, ModelPublish, ModelPublishArgs }
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.scoring.models.DaalNaiveBayesReaderPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.datacatalog.DataCatalogRestResponseJsonProtocol._
import ModelPublishJsonProtocol._
import DaalNaiveBayesModelFormat._

/**
 * Publish a Random Forest Classifier Model for scoring
 */
@PluginDoc(oneLine = "Creates a scoring engine tar file.",
  extended = """Creates a tar file with the trained multinomial Naive Bayes Model
The tar file is used as input to the scoring engine to predict the class of an observation.""",
  returns = """The HDFS path to the tar file.""")
class DaalNaiveBayesPublishPlugin extends CommandPlugin[ModelPublishArgs, ExportMetadata] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:daal_naive_bayes/publish"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

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

    val model: Model = arguments.model
    val naiveBayesData = model.data.convertTo[DaalNaiveBayesModelData]
    val jsvalue: JsValue = naiveBayesData.toJson

    val modelArtifact = ModelPublish.createTarForScoringEngine(jsvalue.toString().getBytes(Charsets.UTF_8),
      "scoring-models", classOf[DaalNaiveBayesReaderPlugin].getName,
      Some(DaalUtils.getDaalLibraryPaths(EngineConfig.daalDynamicLibraries))
    )
    ExportMetadata(modelArtifact.filePath, "model", "tar", modelArtifact.fileSize, model.name.getOrElse("daal_naive_bayes_model"))
  }
}
