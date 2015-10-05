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

package org.trustedanalytics.atk.engine.model.plugins.clustering.lda

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._
import LdaJsonFormat._
/**
 * Train plugin for Latent Dirichlet Allocation
 */
@PluginDoc(oneLine = "Creates Latent Dirichlet Allocation model",
  extended = """See the discussion about `Latent Dirichlet Allocation at Wikipedia. <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`__""",
  returns = """The data returned is composed of multiple components\:

|   **Frame** : *topics_given_doc*
|       Conditional probabilities of topic given document.
|   **Frame** : *word_given_topics*
|       Conditional probabilities of word given topic.
|   **Frame** : *topics_given_word*
|       Conditional probabilities of topic given word.
|   **str** : *report*
|       The configuration and learning curve report for Latent Dirichlet
Allocation as a multiple line str.""")
class LdaTrainPlugin
    extends SparkCommandPlugin[LdaTrainArgs, LdaTrainResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LdaTrainArgs)(implicit invocation: Invocation) = arguments.maxIterations + 5

  override def execute(arguments: LdaTrainArgs)(implicit invocation: Invocation): LdaTrainResult = {

    val frames = engine.frames
    val config = configuration

    // validate arguments
    val edgeFrame: SparkFrame = arguments.frame
    edgeFrame.schema.requireColumnIsType(arguments.documentColumnName, DataTypes.string)
    edgeFrame.schema.requireColumnIsType(arguments.wordColumnName, DataTypes.string)
    edgeFrame.schema.requireColumnIsType(arguments.wordCountColumnName, DataTypes.isIntegerDataType)
    require(edgeFrame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    val ldaModel = LdaFunctions.trainLdaModel(edgeFrame.rdd, arguments)
    val model: Model = arguments.model
    model.data = ldaModel.toJson.asJsObject

    val topicsGivenDocFrame = engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("LDA frame with conditional probabilities of topics given document"))) {
      frame: FrameEntity => frame.save(ldaModel.getTopicsGivenDocFrame)
    }

    val wordGivenTopicsFrame = engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("LDA frame with conditional probabilities of word given topics"))) {
      frame: FrameEntity => frame.save(ldaModel.getWordGivenTopicsFrame)
    }

    val topicsGivenWordFrame = engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("LDA frame with conditional probabilities of topics given word"))) {
      frame: FrameEntity => frame.save(ldaModel.getTopicsGivenWordFrame)
    }

    LdaTrainResult(topicsGivenDocFrame, wordGivenTopicsFrame, topicsGivenWordFrame, "")
  }

}
