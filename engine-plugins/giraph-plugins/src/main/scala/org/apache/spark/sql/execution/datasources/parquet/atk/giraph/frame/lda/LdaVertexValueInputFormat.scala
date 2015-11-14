/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet.atk.giraph.frame.lda

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration
import org.apache.giraph.io.{VertexValueInputFormat, VertexValueReader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.CatalystReadSupport
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.giraph.config.lda.GiraphLdaConfiguration
import org.trustedanalytics.atk.giraph.io.{LdaVertexData, LdaVertexId}
import org.trustedanalytics.atk.giraph.plugins.util.GiraphConfigurationUtil
import java.util

/**
 * Input format for LDA vertex data reads from Parquet Frame
 */
class LdaVertexValueInputFormat extends VertexValueInputFormat[LdaVertexId, LdaVertexData] {
  private val parquetInputFormat = new ParquetInputFormat[InternalRow]()


  override def checkInputSpecs(conf: Configuration): Unit = {
    new GiraphLdaConfiguration(conf).validate()
  }

  override def setConf(conf: ImmutableClassesGiraphConfiguration[LdaVertexId, LdaVertexData, Writable]): Unit = {
    super.setConf(conf)
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)
  }

  override def getSplits(context: JobContext, minSplits: Int): util.List[InputSplit] = {
    setVertexFrameLocation(context.getConfiguration)
    parquetInputFormat.getSplits(context)
  }

  override def createVertexValueReader(split: InputSplit, context: TaskAttemptContext): VertexValueReader[LdaVertexId, LdaVertexData] = {
    setVertexFrameLocation(context.getConfiguration)
    new LdaVertexValueReader(new GiraphLdaConfiguration(context.getConfiguration))
  }

  def setVertexFrameLocation(conf: Configuration): Unit = {
    val ldaConfiguration = new GiraphLdaConfiguration(conf)
    GiraphConfigurationUtil.set(conf, "mapreduce.input.fileinputformat.inputdir",
      Some(ldaConfiguration.ldaConfig.inputFormatConfig.parquetVertexFrameLocation))
  }
}

class LdaVertexValueReader(config: GiraphLdaConfiguration) extends VertexValueReader[LdaVertexId, LdaVertexData] {
  private val ldaConfig = config.ldaConfig
  private val reader = new ParquetRecordReader[InternalRow](new CatalystReadSupport())
  private val row = new RowWrapper(config.ldaConfig.inputFormatConfig.vertexFrameSchema)

  private var vertexId: LdaVertexId = null
  private var vertexData: LdaVertexData = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    context.getConfiguration.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)
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
      val isDocument = row.intValue(ldaConfig.isDocumentColumnName)
      vertexId = isDocument match {
        case 1 => new LdaVertexId(row.longValue(ldaConfig.vertexIdColumnName), true)
        case _ => new LdaVertexId(row.longValue(ldaConfig.vertexIdColumnName), false)
      }

      val originalId = row.stringValue(ldaConfig.vertexOriginalIdColumnName)
      vertexData = new LdaVertexData(originalId)
    }
    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }
}
