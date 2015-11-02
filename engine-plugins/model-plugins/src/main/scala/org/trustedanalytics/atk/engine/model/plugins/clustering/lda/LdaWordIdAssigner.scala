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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }

/**
 * Assigns unique long Ids to words
 *
 * @param edgeFrame Frame of edges between documents and words
 * @param wordColumnName Word column name
 * @param wordCountColumnName Word count column name
 */
case class LdaWordIdAssigner(edgeFrame: FrameRdd,
                             wordColumnName: String,
                             wordCountColumnName: String) {
  require(edgeFrame != null, "edge frame is required")
  require(wordColumnName != null, "word column is required")
  require(wordCountColumnName != null, "word count column is required")

  val LdaWordPrefix: String = "_lda_word_"
  val ldaWordIdColumnName: String = LdaWordPrefix + "id_" + wordColumnName
  val ldaWordColumnName: String = LdaWordPrefix + wordColumnName
  val ldaWordCountColumnName: String = LdaWordPrefix + "total_" + wordCountColumnName

  /**
   * Assign unique Ids to words, and count total occurrences of each word in documents.
   *
   * @return Frame with word Id, word, and total count
   */
  def assignUniqueIds(): FrameRdd = {

    val wordsWithIndex: RDD[Row] = edgeFrame.mapRows(row => {
      (row.stringValue(wordColumnName), row.longValue(wordCountColumnName))
    }).reduceByKey(_ + _)
      .zipWithIndex()
      .map {
        case ((word, count), index) =>
          new GenericRow(Array[Any](index, word, count))
      }

    val schema = FrameSchema(List(
      Column(ldaWordIdColumnName, DataTypes.int64),
      Column(ldaWordColumnName, DataTypes.string),
      Column(ldaWordCountColumnName, DataTypes.int64))
    )

    new FrameRdd(schema, wordsWithIndex)
  }
}
