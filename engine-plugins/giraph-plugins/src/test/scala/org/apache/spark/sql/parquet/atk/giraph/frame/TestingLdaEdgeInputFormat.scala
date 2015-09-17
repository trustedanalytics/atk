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

package org.apache.spark.sql.parquet.atk.giraph.frame

import org.trustedanalytics.atk.giraph.io.{ LdaEdgeData, LdaVertexId }
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.io.formats.TextEdgeInputFormat
import org.apache.giraph.io.{ EdgeReader, ReverseEdgeDuplicator }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{ InputSplit, TaskAttemptContext }

/**
 * InputFormat for testing LDA
 */
class TestingLdaEdgeInputFormat extends TextEdgeInputFormat[LdaVertexId, LdaEdgeData] {

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[LdaVertexId, LdaEdgeData] = {
    val reader = new TextEdgeReaderFromEachLine {

      override def getSourceVertexId(line: Text): LdaVertexId = {
        new LdaVertexId(parseLine(line)._1, true)
      }

      override def getTargetVertexId(line: Text): LdaVertexId = {
        new LdaVertexId(parseLine(line)._2, false)
      }

      override def getValue(line: Text): LdaEdgeData = {
        new LdaEdgeData(parseLine(line)._3)
      }

      private def parseLine(line: Text): (Long, Long, Long) = {
        require(StringUtils.isNotBlank(line.toString), "input cannot be blank, please use 'doc,word,word_count,doc_id,word_id")
        val parts = line.toString.split(',')
        require(parts.length == 5, "please use 'doc,word,word_count,doc_id,word_id' e.g. 'nytimes,jobs,23,1,4'")
        (parts(3).toLong, parts(4).toLong, parts(2).toLong)
      }
    }
    new ReverseEdgeDuplicator(reader)
  }

}
