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

import org.apache.spark.mllib.clustering.AtkLdaModel
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc
import org.apache.commons.lang3.StringUtils
import spray.json.{ JsNumber, JsObject, JsValue, JsonFormat }

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
Column should contain an int32 or int64 value.""") wordCountColumnName: String,
                        @ArgDoc("""The maximum number of iterations that the algorithm will execute.
The valid value range is all positive int.
Default is 20.""") maxIterations: Int = 20,
                        @ArgDoc("""The :term:`hyperparameter` for document-specific distribution over topics.
Mainly used as a smoothing parameter in :term:`Bayesian inference`.
If set to a singleton list List(-1d), then docConcentration is set automatically.
If set to singleton list List(t) where t != -1, then t is replicated to a vector of length k during LDAOptimizer.initialize().
Otherwise, the alpha must be length k.
Currently the EM optimizer only supports symmetric distributions, so all values in the vector should be the same.
Values should be greater than 1.0. Default value is -1.0 indicating automatic setting.""") alpha: Option[List[Double]] = None,
                        @ArgDoc("""The :term:`hyperparameter` for word-specific distribution over topics.
Mainly used as a smoothing parameter in :term:`Bayesian inference`.
Larger value implies that topics contain all words more uniformly and
smaller value implies that topics are more concentrated on a small
subset of words.
Valid value range is all positive float greater than or equal to 1.
Default is 0.1.""") beta: Float = 1.1f,
                        @ArgDoc("""The number of topics to identify in the LDA model.
Using fewer topics will speed up the computation, but the extracted topics
might be more abstract or less specific; using more topics will
result in more computation but lead to more specific topics.
Valid value range is all positive int.
Default is 10.""") numTopics: Int = 10,
                        @ArgDoc(
                          """An optional random seed.
The random seed is used to initialize the pseudorandom number generator
used in the LDA model. Setting the random seed to the same value every
time the model is trained, allows LDA to generate the same topic distribution
if the corpus and LDA parameters are unchanged.""") randomSeed: Option[Long] = None) {

  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(StringUtils.isNotBlank(documentColumnName), "document column name is required")
  require(StringUtils.isNotBlank(wordColumnName), "word column name is required")
  require(maxIterations > 0, "Max iterations should be greater than 0")
  if (alpha.isDefined) {
    if (alpha.get.size == 1) {
      require(alpha.get.head == -1d || alpha.get.head > 1d, "Alpha should be greater than 1.0. Or -1.0 indicating default setting ")
    }
    else {
      require(alpha.get.forall(a => a > 1d), "All values of alpha should be greater than 0")
    }
  }
  require(beta > 0, "Beta should be greater than 0")
  require(numTopics > 0, "Number of topics (K) should be greater than 0")

  def columnNames: List[String] = {
    List(documentColumnName, wordColumnName, wordCountColumnName)
  }

  def getAlpha: List[Double] = {
    alpha.getOrElse(List(-1d))
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
  import spray.json._
  implicit val ldaFormat = jsonFormat10(LdaTrainArgs)
  implicit val ldaResultFormat = jsonFormat4(LdaTrainResult)
  implicit val ldaPredictArgsFormat = jsonFormat2(LdaModelPredictArgs)
  implicit val ldaPredictReturnFormat = jsonFormat3(LdaModelPredictReturn)

  implicit object AtkLdaModelFormat extends JsonFormat[AtkLdaModel] {

    override def write(obj: AtkLdaModel): JsValue = {
      JsObject(
        "num_topics" -> JsNumber(obj.numTopics),
        "topic_word_map" -> obj.topicWordMap.toJson
      )
    }

    override def read(json: JsValue): AtkLdaModel = {
      val fields = json.asJsObject.fields
      val numTopics = getOrInvalid(fields, "num_topics").convertTo[Int]
      val topicWordMap = getOrInvalid(fields, "topic_word_map").convertTo[Map[String, Vector[Double]]]
      val ldaModel = new AtkLdaModel(numTopics)
      ldaModel.topicWordMap = topicWordMap
      ldaModel
    }
  }
}

