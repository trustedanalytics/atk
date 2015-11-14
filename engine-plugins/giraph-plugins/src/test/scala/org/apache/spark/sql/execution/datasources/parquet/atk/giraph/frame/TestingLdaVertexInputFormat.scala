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

package org.apache.spark.sql.execution.datasources.parquet.atk.giraph.frame

import org.apache.commons.lang3.StringUtils
import org.apache.giraph.io.formats.{ TextVertexValueInputFormat, TextEdgeInputFormat }
import org.apache.giraph.io.{ EdgeReader, ReverseEdgeDuplicator }
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{ InputSplit, TaskAttemptContext }
import org.trustedanalytics.atk.giraph.io.{ LdaVertexData, LdaEdgeData, LdaVertexId }

/**
 * InputFormat for testing LDA
 */
class TestingLdaVertexInputFormat extends TextVertexValueInputFormat[LdaVertexId, LdaVertexData, LdaEdgeData] {

  override def createVertexValueReader(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): TextVertexValueReader = {
    val reader = new TextVertexValueReaderFromEachLine {

      override def getValue(text: Text): LdaVertexData = {
        val (id, description, isDocument) = parseLine(text)
        new LdaVertexData(description)
      }

      override def getId(text: Text): LdaVertexId = {
        val (id, description, isDocument) = parseLine(text)
        new LdaVertexId(id, isDocument == 1)
      }

      private def parseLine(line: Text): (Long, String, Int) = {
        require(StringUtils.isNotBlank(line.toString), "input cannot be blank, please use 'vertex_id,vertex_description,is_document'")
        val parts = line.toString.split(',')
        require(parts.length == 3, "please use 'vertex_id,vertex_description,is_document' e.g. '22,nytimes,1'")
        (parts(0).toLong, parts(1), parts(2).toInt)
      }
    }
    reader.asInstanceOf[TextVertexValueReader]
  }
}

