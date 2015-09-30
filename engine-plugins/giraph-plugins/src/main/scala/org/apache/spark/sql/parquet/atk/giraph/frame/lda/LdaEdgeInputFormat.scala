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

package org.apache.spark.sql.parquet.atk.giraph.frame.lda

import java.util

import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.giraph.io.{ LdaEdgeData, LdaVertexId }
import org.trustedanalytics.atk.giraph.config.lda.LdaConfiguration
import org.apache.giraph.edge.{ DefaultEdge, Edge }
import org.apache.giraph.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{ InputSplit, JobContext, TaskAttemptContext }
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetInputFormat, ParquetRecordReader }

/**
 * InputFormat for LDA reads edges from Parquet Frame
 */
class LdaParquetFrameEdgeInputFormat extends EdgeInputFormat[LdaVertexId, LdaEdgeData] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  override def checkInputSpecs(conf: Configuration): Unit = {
    new LdaConfiguration(conf).validate()
  }

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[LdaVertexId, LdaEdgeData] = {
    val ldaEdgeReader = new LdaParquetFrameEdgeReader(new LdaConfiguration(context.getConfiguration))
    // algorithm expects edges that go both ways (seems to be how undirected is modeled in Giraph)
    new ReverseEdgeDuplicator[LdaVertexId, LdaEdgeData](ldaEdgeReader)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): util.List[InputSplit] = {
    parquetInputFormat.getSplits(context)
  }
}

class LdaParquetFrameEdgeReader(config: LdaConfiguration) extends EdgeReader[LdaVertexId, LdaEdgeData] {

  private val ldaConfig = config.ldaConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.ldaConfig.inputFormatConfig.edgeFrameSchema)

  private var currentSourceId: LdaVertexId = null
  private var currentEdge: DefaultEdge[LdaVertexId, LdaEdgeData] = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentEdge: Edge[LdaVertexId, LdaEdgeData] = {
    currentEdge
  }

  override def getCurrentSourceId: LdaVertexId = {
    currentSourceId
  }

  override def nextEdge(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val docId = new LdaVertexId(row.longValue(ldaConfig.documentIdColumnName), true)
      val wordId = new LdaVertexId(row.longValue(ldaConfig.wordIdColumnName), false)
      val wordCount = row.longValue(ldaConfig.wordCountColumnName)

      currentSourceId = docId

      currentEdge = new DefaultEdge[LdaVertexId, LdaEdgeData]()
      currentEdge.setTargetVertexId(wordId)
      currentEdge.setValue(new LdaEdgeData(wordCount.toDouble))
    }
    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }

}
