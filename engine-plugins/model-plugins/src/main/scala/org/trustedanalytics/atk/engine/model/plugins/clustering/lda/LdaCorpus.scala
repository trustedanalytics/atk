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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.{Column, DataTypes}

/**
 * Corpus of documents for training LDA model
 *
 * LDA corpus is represented as document Ids, and corresponding word count vectors.
 * Each word count vector is a "bags of words" with a fixed-size vocabulary.
 * The  vocabulary size is the length of the vector.
 *
 * @param edgeFrame Frame of edges between documents and words
 * @param args LDA train arguments
 */
case class LdaCorpus(edgeFrame: FrameRdd, args: LdaTrainArgs) {
  require(edgeFrame != null, "edge frame is required")
  require(args != null, "LDA train arguments required")

  lazy val wordIdAssigner = LdaWordIdAssigner(edgeFrame, args.wordColumnName, args.wordCountColumnName)
  lazy val uniqueWordsFrame = wordIdAssigner.assignUniqueIds().cache()

  /**
   * Create corpus of documents for training LDA model
   *
   * @return RDD of document Id, document name, and word count vector
   */
  def createCorpus(): RDD[(Long, (String, Vector))] = {
    val wordCount = uniqueWordsFrame.count().toInt
    val edgeFrameWithWordIds = addWordIdsToEdgeFrame()

    val docWordRdd = edgeFrameWithWordIds.mapRows(row => {
      val document = row.stringValue(args.documentColumnName)
      val wordId = row.longValue(wordIdAssigner.ldaWordIdColumnName)
      val wordCount = row.longValue(args.wordCountColumnName)
      (document, (wordId, wordCount))
    })

    val corpus: RDD[(String, Vector)] = docWordRdd.aggregateByKey(
      Map[Long, Long]())((wordCountMap, value) => value match {
      case (wordId, wordCount) => wordCountMap + (wordId -> wordCount)
    }, (map1, map2) => map1 ++ map2).map {
      case (document, wordCountMap: Map[Long, Long]) =>
        val wordIndices = wordCountMap.keys.map(_.toInt).toArray
        val wordCountValues = wordCountMap.values.map(_.toDouble).toArray
        (document, new SparseVector(wordCount, wordIndices, wordCountValues))
    }

    corpus.zipWithIndex.map { case ((document, wordVector), documentId) =>
      (documentId, (document, wordVector))
    }
  }

  /**
   * Add word Ids to edge frame
   *
   * @return Edge frame with word Ids
   */
  private[clustering] def addWordIdsToEdgeFrame(): FrameRdd = {
    val edgeDataFrame = edgeFrame.toDataFrame
    val wordDataFrame = uniqueWordsFrame.toDataFrame

    val joinedDataFrame: RDD[Row] = edgeDataFrame.join(
      wordDataFrame,
      edgeDataFrame(args.wordColumnName) === wordDataFrame(wordIdAssigner.ldaWordColumnName)
    ).select(
        args.documentColumnName,
        args.wordColumnName,
        args.wordCountColumnName,
        wordIdAssigner.ldaWordIdColumnName)
      .map(row => new GenericRow(row.toSeq.toArray)) //TODO: Delete the conversion from GenericRowWithSchema to GenericRow once we upgrade to Spark1.4

    val joinedSchema = edgeFrame.frameSchema.addColumn(Column(wordIdAssigner.ldaWordIdColumnName, DataTypes.int64))
    new FrameRdd(joinedSchema, joinedDataFrame)
  }
}
