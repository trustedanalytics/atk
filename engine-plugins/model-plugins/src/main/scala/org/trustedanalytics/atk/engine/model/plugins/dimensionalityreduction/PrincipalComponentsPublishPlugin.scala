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

package org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction

import org.trustedanalytics.atk.domain.StringValue
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import MLLibJsonProtocol._
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.scoring.{ ModelPublishJsonProtocol, ModelPublish, ModelPublishArgs }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, CommandPlugin, ApiMaturityTag, Invocation }

// Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import ModelPublishJsonProtocol._
import spray.json._
/**
 * Publish a Principal Components Model for scoring
 */
@PluginDoc(oneLine = "Creates a tar file that will be used as input to the scoring engine",
  extended =
    """The publish method exports the PrincipalComponentsModel and its implementation into a tar file. The tar file is then published
on HDFS and this method returns the path to the tar file. The tar file serves as input to the scoring engine. This model can
then be used to compute the principal components and t-squared index(if requested) of an observation.""",
  returns = """Returns the HDFS path to the trained model's tar file""")
class PrincipalComponentsPublishPlugin extends CommandPlugin[ModelPublishArgs, StringValue] {

  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:principal_components/publish"

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
  override def execute(arguments: ModelPublishArgs)(implicit invocation: Invocation): StringValue = {

    val model: Model = arguments.model

    StringValue(ModelPublish.createTarForScoringEngine(model.data.toString(),
      "scoring-models",
      "org.trustedanalytics.atk.scoring.models.PrincipalComponentsReaderPlugin"))
  }
}

