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

package org.trustedanalytics.atk.giraph.config.lda

import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc
import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.giraph.plugins.model.lda.LdaModel

/**
 * Arguments to the LDA train plugin - see user docs for more on the parameters
 */
case class LdaTrainArgs(model: ModelReference,
                        @ArgDoc("""Input frame data.""") frame: FrameReference,
                        @ArgDoc("""Column Name for documents.
Column should contain a str value.""") documentColumnName: String,
                        @ArgDoc("""Column name for words.
Column should contain a str value.""") wordColumnName: String,
                        @ArgDoc("""Column name for word count.
Column should contain an int64 value.""") wordCountColumnName: String,
                        @ArgDoc("""The maximum number of iterations that the algorithm will execute.
The valid value range is all positive int.
Default is 20.""") maxIterations: Option[Int] = None,
                        @ArgDoc("""The hyper-parameter for document-specific distribution over topics.
Mainly used as a smoothing parameter in :term:`Bayesian inference`.
Larger value implies that documents are assumed to cover all topics
more uniformly; smaller value implies that documents are more
concentrated on a small subset of topics.
Valid value range is all positive float.
 Default is 0.1.""") alpha: Option[Float] = None,
                        @ArgDoc("""The hyper-parameter for word-specific distribution over topics.
Mainly used as a smoothing parameter in :term:`Bayesian inference`.
Larger value implies that topics contain all words more uniformly and
smaller value implies that topics are more concentrated on a small
subset of words.
Valid value range is all positive float.
Default is 0.1.""") beta: Option[Float] = None,
                        @ArgDoc("""The amount of change in LDA model parameters that will be tolerated
at convergence.
If the change is less than this threshold, the algorithm exits
before it reaches the maximum number of supersteps.
Valid value range is all positive float and 0.0.
Default is 0.001.""") convergenceThreshold: Option[Float] = None,
                        @ArgDoc(""""True" means turn on cost evaluation and "False" means turn off
cost evaluation.
It's relatively expensive for LDA to evaluate cost function.
For time-critical applications, this option allows user to turn off cost
function evaluation.
Default is "False".""") evaluateCost: Option[Boolean] = None,
                        @ArgDoc("""The number of topics to identify in the LDA model.
Using fewer topics will speed up the computation, but the extracted topics
might be more abstract or less specific; using more topics will
result in more computation but lead to more specific topics.
Valid value range is all positive int.
Default is 10.""") numTopics: Option[Int] = None) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(documentColumnName), "document column name is required")
  require(StringUtils.isNotBlank(wordColumnName), "word column name is required")
  require(StringUtils.isNotBlank(wordCountColumnName), "word count column name is required")
  require(maxIterations.isEmpty || maxIterations.get > 0, "Max iterations should be greater than 0")
  require(alpha.isEmpty || alpha.get > 0, "Alpha should be greater than 0")
  require(beta.isEmpty || beta.get > 0, "Beta should be greater than 0")
  require(convergenceThreshold.isEmpty || convergenceThreshold.get >= 0, "Convergence threshold should be greater than or equal to 0")
  require(numTopics.isEmpty || numTopics.get > 0, "Number of topics (K) should be greater than 0")

  def columnNames: List[String] = {
    List(documentColumnName, wordColumnName, wordCountColumnName)
  }

  def getMaxIterations: Int = {
    maxIterations.getOrElse(20)
  }

  def getAlpha: Float = {
    alpha.getOrElse(0.1f)
  }

  def getBeta: Float = {
    beta.getOrElse(0.1f)
  }

  def getConvergenceThreshold: Float = {
    convergenceThreshold.getOrElse(0.001f)
  }

  def getEvaluateCost: Boolean = {
    evaluateCost.getOrElse(false)
  }

  def getNumTopics: Int = {
    numTopics.getOrElse(10)
  }

}

/**
 * Return arguments to the LDA train plugin
 *
 * @param topicsGivenDoc Frame with conditional probabilities of topic given document
 * @param wordGivenTopics Frame with conditional probabilities of word given topic
 * @param topicsGivenWord Frame with conditional probabilities of topic given word
 * @param report Configuration and learning curve report for LDA as a multiple line string
 */
case class LdaTrainResult(topicsGivenDoc: FrameEntity, wordGivenTopics: FrameEntity, topicsGivenWord: FrameEntity, report: String) {
  require(topicsGivenDoc != null, "topic given document frame is required")
  require(wordGivenTopics != null, "word given topic frame is required")
  require(topicsGivenWord != null, "topic given word frame is required")
  require(StringUtils.isNotBlank(report), "report is required")
}

/**
 * Arguments to the LDA predict plugin - see user docs for more on the parameters
 */
case class LdaModelPredictArgs(@ArgDoc("""Reference to the model for which topics are to be determined.""") model: ModelReference,
                               @ArgDoc("""Document whose topics are to be predicted. """) document: List[String])

/**
 * Return arguments to the LDA predict plugin
 *
 * @param topicsGivenDoc Vector of conditional probabilities of topics given document
 * @param newWordsCount Count of new words in test document not present in training set
 * @param newWordsPercentage Percentage of new word in test document
 */
case class LdaModelPredictReturn(topicsGivenDoc: Vector[Double],
                                 newWordsCount: Int,
                                 newWordsPercentage: Double)

/** Json conversion for arguments and return value case classes */
object LdaJsonFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val ldaFormat = jsonFormat11(LdaTrainArgs)
  implicit val ldaResultFormat = jsonFormat4(LdaTrainResult)
  implicit val ldaModelFormat = jsonFormat2(LdaModel.apply)
  implicit val ldaPredictArgsFormat = jsonFormat2(LdaModelPredictArgs)
  implicit val ldaPredictReturnFormat = jsonFormat3(LdaModelPredictReturn)
}

