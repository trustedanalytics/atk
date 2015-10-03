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
package org.apache.spark.mllib.clustering

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.clustering.LDA.TopicCounts
import org.apache.spark.mllib.linalg.{DenseVector => MlDenseVector, Matrix, Vector => MlVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.{Column, DataTypes, FrameSchema}
import org.trustedanalytics.atk.engine.model.plugins.clustering.lda.LdaModelPredictReturn

import scala.collection.immutable.Map
import scala.util.Try

/**
 * Model for Latent Dirichlet Allocation
 *
 * @param numTopics Number of topics in trained model
 */
case class AtkLdaModel(numTopics: Int) {
  require(numTopics > 0, "number of topics must be greater than zero")

  val topicWordMap = Map[String, Vector[Double]]()

  /** Trained LDA model */
  private var distLdaModel: DistributedLDAModel = null

  /** Frame with conditional probabilities of topics given document */
  private var topicsGivenDocFrame: FrameRdd = null

  /** Frame with conditional probabilities of word given topics */
  private var wordGivenTopicsFrame: FrameRdd = null

  /** Frame with conditional probabilities of topics given word */
  private var topicsGivenWordFrame: FrameRdd = null

  /**
   * Create ATK LDA model 
   * @param distLdaModel Trained LDA model 
   */
  def this(distLdaModel: DistributedLDAModel) = {
    this(distLdaModel.k)
    this.distLdaModel = distLdaModel
  }

  /**
   * Get frame with conditional probabilities of topics given word
   */
  def getTopicsGivenWordFrame: FrameRdd = {
    require(this.topicsGivenWordFrame != null, "topics given word frame is not initialized.")
    this.topicsGivenWordFrame
  }

  /**
   * Get frame with conditional probabilities of word given topics
   */
  def getWordGivenTopicsFrame: FrameRdd = {
    require(this.wordGivenTopicsFrame != null, "word given topics frame is not initialized.")
    this.wordGivenTopicsFrame
  }

  /**
   * Get frame with conditional probabilities of topics given document
   */
  def getTopicsGivenDocFrame: FrameRdd = {
    require(this.topicsGivenDocFrame != null, "topics given document frame is not initialized.")
    this.topicsGivenDocFrame
  }

  /**
   * Set frames with conditional probabilities of word given topics, and topics given words
   *
   * Calculates the conditional probabilities of word given topics, and topics given words
   * using the topics matrix. The topics matrix contains the counts of words in topics.
   * The method also joins words in the unique word frame with the word Ids in the topics matrix.
   *
   * @param uniqueWordsFrame Input frame of unique words and counts
   * @param inputWordIdColumnName Name of word Id column in input frame
   * @param inputWordColumnName Name of word column in input frame
   * @param inputWordCountColumnName Name of word count column in input frame
   * @param outputWordColumnName Name of word column in output frame
   * @param outputTopicVectorColumnName Name of vector of conditional probabilities in output frame
   */
  def setWordTopicFrames(uniqueWordsFrame: FrameRdd,
                         inputWordIdColumnName: String,
                         inputWordColumnName: String,
                         inputWordCountColumnName: String,
                         outputWordColumnName: String,
                         outputTopicVectorColumnName: String): Unit = {
    require(distLdaModel != null, "Trained LDA model must not be null")

    val topicsMatrix = distLdaModel.topicsMatrix
    val broadcastTopicsMap = broadcastTopicsMatrix(uniqueWordsFrame.sparkContext, topicsMatrix)
    val globalTopicCounts = getGlobalTopicCounts //Nk in Asuncion 2009 paper
    val eta1 = getTopicConcentration - 1
    val scaledVocabSize = distLdaModel.vocabSize * eta1

    val wordTopicsRdd = uniqueWordsFrame.mapRows(row => {
      val wordId = row.longValue(inputWordIdColumnName)
      val word = row.stringValue(inputWordColumnName)
      val wordCount = row.longValue(inputWordCountColumnName)
      val topicVector = broadcastTopicsMap.value(wordId)

      val wordGivenTopics = calcWordGivenTopicProb(topicVector, globalTopicCounts, scaledVocabSize, eta1)
      val topicsGivenWord = calcTopicsGivenWord(topicVector, wordCount)
      (word, (wordGivenTopics, topicsGivenWord))
    })

    setWordGivenTopicsFrame(wordTopicsRdd, outputWordColumnName, outputTopicVectorColumnName)
    setTopicsGivenWordFrame(wordTopicsRdd, outputWordColumnName, outputTopicVectorColumnName)
  }

  /**
   * Set frame with conditional probabilities of topics given document
   *
   * @param corpus  LDA corpus with document Id, document name, and word count vector
   * @param outputDocumentColumnName Name of document column in output frame
   * @param outputTopicVectorColumnName Name of vector of conditional probabilities in output frame
   */
  def setDocTopicFrame(corpus: RDD[(Long, (String, MlVector))],
                       outputDocumentColumnName: String,
                       outputTopicVectorColumnName: String): Unit = {
    val topicDist = distLdaModel.topicDistributions
    val topicsGivenDocs: RDD[Row] = corpus.join(topicDist).map { case (documentId, ((document, wordVector), topicVector)) =>
      new GenericRow(Array[Any](document, topicVector.toArray))
    }

    val schema = FrameSchema(List(
      Column(outputDocumentColumnName, DataTypes.string),
      Column(outputTopicVectorColumnName, DataTypes.vector(numTopics))))

    this.topicsGivenDocFrame = new FrameRdd(schema, topicsGivenDocs)
  }

  /**
   * Predict conditional probabilities of topics given document
   *
   * @param document Test document represented as a list of words
   * @return Topic predictions for document
   */
  def predict(document: List[String]): LdaModelPredictReturn = {
    require(document != null, "document must not be null")

    val docLength = document.length
    val wordOccurrences: Map[String, Int] = computeWordOccurrences(document)
    val topicGivenDoc = new Array[Double](numTopics)

    for (word <- wordOccurrences.keys) {
      val wordGivenDoc = wordProbabilityGivenDocument(word, wordOccurrences, docLength)
      if (topicWordMap.contains(word)) {
        val topicGivenWord = topicWordMap(word)
        for (i <- topicGivenDoc.indices) {
          topicGivenDoc(i) += topicGivenWord(i) * wordGivenDoc
        }
      }
    }

    val newWordCount = computeNewWordCount(document)
    val percentOfNewWords = computeNewWordPercentage(newWordCount, docLength)
    new LdaModelPredictReturn(topicGivenDoc.toVector, newWordCount, percentOfNewWords)
  }

  /**
   * Compute counts for each word
   *
   * @param document Test document represented as a list of words
   * @return Map with counts for each word
   */
  def computeWordOccurrences(document: List[String]): Map[String, Int] = {
    require(document != null, "document must not be null")
    var wordOccurrences: Map[String, Int] = Map[String, Int]()
    for (word <- document) {
      val count = wordOccurrences.getOrElse(word, 0) + 1
      wordOccurrences += (word -> count)
    }
    wordOccurrences
  }

  /**
   * Compute conditional probability of word given document
   *
   * @param word Input word
   * @param wordOccurrences Number of occurrences of word in document
   * @param docLength Total number of words in document
   * @return Conditional probability of word given document
   */
  def wordProbabilityGivenDocument(word: String,
                                   wordOccurrences: Map[String, Int],
                                   docLength: Int): Double = {
    require(docLength >= 0, "number of words in document must be greater than or equal to zero")
    val wordCount = wordOccurrences.getOrElse(word, 0)
    if (docLength > 0) wordCount.toDouble / docLength else 0d
  }

  /**
   * Compute conditional probability of topic given word
   */
  def topicProbabilityGivenWord(word: String, topicIndex: Int): Double = {
    if (topicWordMap.contains(word)) {
      topicWordMap(word)(topicIndex)
    }
    else 0d
  }

  /**
   * Compute count of new words in document not present in trained model
   *
   * @param document Test document
   * @return Count of new words in document
   */
  def computeNewWordCount(document: List[String]): Int = {
    require(document != null, "document must not be null")
    var count = 0
    for (word <- document) {
      if (!topicWordMap.contains(word))
        count += 1
    }
    count
  }

  /**
   * Compute percentage of new words in document not present in trained model
   *
   * @param newWordCount Count of new words in document
   * @param docLength Total number of words in document
   * @return  Count of new words in document
   */
  def computeNewWordPercentage(newWordCount: Int, docLength: Int): Double = {
    require(docLength >= 0, "number of words in document must be greater than or equal to zero")
    if (docLength > 0) newWordCount * 100 / docLength.toDouble else 0d
  }

  /**
   * Set frame of conditional probabilities of words given topics
   *
   * @param wordTopicsRdd RDD of word, word given topic vector, and topic given word vector
   * @param wordColumnName Word column name
   * @param topicVectorColumnName Topic vector column name
   */
  private[clustering] def setWordGivenTopicsFrame(wordTopicsRdd: RDD[(String, (MlVector, MlVector))],
                                                  wordColumnName: String,
                                                  topicVectorColumnName: String): Unit = {
    val frameSchema = FrameSchema(List(
      Column(wordColumnName, DataTypes.string),
      Column(topicVectorColumnName, DataTypes.vector(numTopics))
    ))

    val wordGivenTopicRows: RDD[Row] = wordTopicsRdd.map { case ((word, (wordGivenTopics, topicsGivenWord))) =>
      new GenericRow(Array[Any](word, wordGivenTopics))
    }

    this.wordGivenTopicsFrame = new FrameRdd(frameSchema, wordGivenTopicRows)
  }

  /**
   * Set frame of conditional probabilities of topics given word
   *
   * @param wordTopicsRdd RDD of word, word given topic vector, and topic given word vector
   * @param wordColumnName Word column name
   * @param topicVectorColumnName Topic vector column name
   */
  private[clustering] def setTopicsGivenWordFrame(wordTopicsRdd: RDD[(String, (MlVector, MlVector))],
                                                  wordColumnName: String,
                                                  topicVectorColumnName: String): Unit = {
    val frameSchema = FrameSchema(List(
      Column(wordColumnName, DataTypes.string),
      Column(topicVectorColumnName, DataTypes.vector(numTopics))
    ))

    val topicsGivenWordRows: RDD[Row] = wordTopicsRdd.map { case ((word, (wordGivenTopics, topicsGivenWord))) =>
      new GenericRow(Array[Any](word, topicsGivenWord))
    }

    this.topicsGivenWordFrame = new FrameRdd(frameSchema, topicsGivenWordRows)
  }

  /**
   * Calculate conditional probability of word given topics
   *
   * @param topicVector Vector with counts of word in topics
   * @param globalTopicCounts Global topic counts
   * @param scaledVocabSize Vocabulary size * (eta - 1)
   * @param eta1 Topic concentration minus 1 (eta - 1)
   * @return Vector with conditional probability of word given topics
   */
  private[clustering] def calcWordGivenTopicProb(topicVector: MlVector,
                                                 globalTopicCounts: TopicCounts,
                                                 scaledVocabSize: Double,
                                                 eta1: Double): MlVector = {
    val wordGivenTopic = topicVector.copy.toArray
    var k = 0
    while (k < wordGivenTopic.size) {
      // (Nwk + eta -1 )/(Nk + W*eta - W) in Asuncion 2009
      wordGivenTopic(k) = (wordGivenTopic(k) + eta1) / (globalTopicCounts(k) + scaledVocabSize)
      k += 1
    }
    new MlDenseVector(wordGivenTopic)
  }

  /**
   * Calculate conditional probability of topics given word
   *
   * @param topicVector Vector with counts of word in topics
   * @param wordCount Count of word in corpus
   * @return Vector with conditional probability of topics given word
   */
  private[clustering] def calcTopicsGivenWord(topicVector: MlVector, wordCount: Long): MlVector = {
    val topicGivenWord = topicVector.copy.toArray
    var k = 0
    while (k < topicGivenWord.size) {
      topicGivenWord(k) = topicGivenWord(k) / wordCount
      k += 1
    }
    new MlDenseVector(topicGivenWord)
  }

  /**
   * Create broadcast variable from topics matrix
   *
   * @param sparkContext Spark context
   * @param topicsMatrix Topic matrix
   *
   * @return Broadcast variable with map of word Ids and topic vectors
   */
  private[clustering] def broadcastTopicsMatrix(sparkContext: SparkContext,
                                                topicsMatrix: Matrix): Broadcast[Map[Long, MlVector]] = {
    val topicsMatrix = distLdaModel.topicsMatrix
    var topicsMap = Map[Long, MlVector]()

    for (w <- 0 until topicsMatrix.numRows) {
      val topicArr = Array.fill(numTopics)(0d)
      for (k <- 0 until distLdaModel.k) {
        topicArr(k) = topicsMatrix(w, k)
      }
      topicsMap += w.toLong -> new MlDenseVector(topicArr)
    }
    sparkContext.broadcast(topicsMap)
  }

  /**
   * Get global topic counts (N_k in Asuncion et al. (2009) paper)
   */
  private[clustering] def getGlobalTopicCounts: TopicCounts = {
    //TODO: Delete reflection once these private variables are accessible in Spark1.4.+
    val privateField = classOf[DistributedLDAModel].getDeclaredField("globalTopicTotals")
    Try(privateField.setAccessible(true)).orElse(
      throw new SecurityException("Cannot access global topic counts in distributed LDA model.")
    )
    privateField.get(distLdaModel).asInstanceOf[TopicCounts]
  }

  /**
   * Get topic concentration (beta)
   */
  private[clustering] def getTopicConcentration: Double = {
    //TODO: Delete reflection once these private variables are accessible in Spark1.4.+
    val privateField = classOf[DistributedLDAModel].getDeclaredField("topicConcentration")
    Try(privateField.setAccessible(true)).orElse(
      throw new SecurityException("Cannot access topic concentration in distributed LDA model.")
    )
    privateField.get(distLdaModel).asInstanceOf[Double]
  }
}

