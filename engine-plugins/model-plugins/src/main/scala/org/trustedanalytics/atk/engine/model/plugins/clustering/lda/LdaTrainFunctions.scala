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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.clustering.{ LDAModel, AtkLdaModel, DistributedLDAModel, LDA }
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object LdaTrainFunctions extends Serializable {

  /**
   * Train LDA model
   *
   * @param edgeFrame Frame of edges between documents and words
   * @param args LDA train arguments
   * @return Trained LDA model
   */
  def trainLdaModel(edgeFrame: FrameRdd, args: LdaTrainArgs): AtkLdaModel = {
    val ldaCorpus = LdaCorpus(edgeFrame, args)
    val trainCorpus = ldaCorpus.createCorpus().cache()
    val distLdaModel = runLda(trainCorpus, args)

    val ldaModel = new AtkLdaModel(distLdaModel)
    ldaModel.setDocTopicFrame(trainCorpus, args.documentColumnName, "topic_probabilities")
    ldaModel.setWordTopicFrames(
      ldaCorpus.uniqueWordsFrame,
      ldaCorpus.wordIdAssigner.ldaWordIdColumnName,
      ldaCorpus.wordIdAssigner.ldaWordColumnName,
      ldaCorpus.wordIdAssigner.ldaWordCountColumnName,
      args.wordColumnName,
      "topic_probabilities"
    )
    ldaModel
  }

  /**
   * Initialize LDA runner with training arguments supplied by user
   *
   * @param args LDA train arguments
   * @return LDA runner
   */
  private[clustering] def initializeLdaRunner(args: LdaTrainArgs): LDA = {
    val ldaRunner = new LDA()
    ldaRunner.setDocConcentration(args.alpha)
    ldaRunner.setTopicConcentration(args.beta)
    ldaRunner.setMaxIterations(args.maxIterations)
    ldaRunner.setK(args.numTopics)
    if (args.randomSeed.isDefined) {
      ldaRunner.setSeed(args.randomSeed.get)
    }
    ldaRunner
  }

  /**
   * Run LDA model
   * @param corpus LDA corpus of document Ids, document names, and corresponding word count vectors.
   *               The length of each word count vector is the vocabulary size.
   * @param args LDA train arguments
   * @return Trained LDA model
   */
  private[clustering] def runLda(corpus: RDD[(Long, (String, Vector))], args: LdaTrainArgs): DistributedLDAModel = {
    val ldaCorpus = corpus.map { case ((documentId, (document, wordVector))) => (documentId, wordVector) }
    val ldaModel = initializeLdaRunner(args).run(ldaCorpus)
    ldaModel match {
      case m: DistributedLDAModel => m
      case _ => throw new RuntimeException("Local LDA models are not supported.")
    }
  }
}
