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

import org.apache.giraph.io.{VertexValueReader, VertexValueInputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit, JobContext}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.parquet.RowReadSupport
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.giraph.config.lda.LdaConfiguration
import org.trustedanalytics.atk.giraph.io.{LdaVertexData, LdaVertexId}
import parquet.hadoop.{ParquetRecordReader, ParquetInputFormat}

/**
 * Input format for LDA vertex data reads from Parquet Frame
 */
class LdaVertexValueInputFormat extends VertexValueInputFormat[LdaVertexId, LdaVertexData ]{
  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])


  override def checkInputSpecs(conf: Configuration): Unit = {
    new LdaConfiguration(conf).validate()
  }

  override def getSplits(context: JobContext, minSplits: Int): util.List[InputSplit] = {
    parquetInputFormat.getSplits(context)
  }

  override def createVertexValueReader(split: InputSplit, context: TaskAttemptContext): VertexValueReader[LdaVertexId, LdaVertexData] = {
    new LdaVertexValueReader(new LdaConfiguration(context.getConfiguration))
  }
}

class LdaVertexValueReader(config: LdaConfiguration) extends VertexValueReader[LdaVertexId, LdaVertexData] {
  private val ldaConfig = config.ldaConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.ldaConfig.inputFormatConfig.frameSchema)

  private var vertexId: LdaVertexId = null
  private var vertexData : LdaVertexData = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentVertexId: LdaVertexId = {
    vertexId
  }

  override def getCurrentVertexValue: LdaVertexData = {
    vertexData
  }

  override def nextVertex(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)
      val isDocument = row.booleanValue(ldaConfig.isDocumentColumnName)

      vertexId = isDocument match {
        case true => new LdaVertexId(row.longValue(ldaConfig.vertexIdColumnName), true)
        case _ =>new LdaVertexId(row.longValue(ldaConfig.vertexIdColumnName), false)
      }

      val description = row.stringValue(ldaConfig.vertexDescriptionColumnName)
      vertexData = new LdaVertexData(description)
    }
    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }
}
