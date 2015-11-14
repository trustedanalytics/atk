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

package org.apache.spark.sql.execution.datasources.parquet.atk.giraph.frame.cf

import java.util

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration
import org.apache.giraph.edge.{DefaultEdge, Edge}
import org.apache.giraph.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetRecordReader}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.CatalystReadSupport
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.giraph.config.cf.CollaborativeFilteringConfiguration
import org.trustedanalytics.atk.giraph.io.EdgeData4CFWritable.EdgeType
import org.trustedanalytics.atk.giraph.io.{CFVertexId, EdgeData4CFWritable}

/**
 * InputFormat for LDA reads edges from Parquet Frame
 */
class CollaborativeFilteringEdgeInputFormat extends EdgeInputFormat[CFVertexId, EdgeData4CFWritable] {

  private val parquetInputFormat = new ParquetInputFormat[InternalRow]()

  override def checkInputSpecs(conf: Configuration): Unit = {
    new CollaborativeFilteringConfiguration(conf).validate()
  }

  override def setConf(conf: ImmutableClassesGiraphConfiguration[CFVertexId, Writable, EdgeData4CFWritable]): Unit = {
    super.setConf(conf)
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)
  }

  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[CFVertexId, EdgeData4CFWritable] = {
    val edgeReader = new CollaborativeFilteringEdgeReader(new CollaborativeFilteringConfiguration(context.getConfiguration))

    // algorithm expects edges that go both ways (seems to be how undirected is modeled in Giraph)
    new ReverseEdgeDuplicator[CFVertexId, EdgeData4CFWritable](edgeReader)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): util.List[InputSplit] = {
    parquetInputFormat.getSplits(context)
  }
}

class CollaborativeFilteringEdgeReader(conf: CollaborativeFilteringConfiguration) extends EdgeReader[CFVertexId, EdgeData4CFWritable] {

  private val config = conf.getConfig
  private val reader = new ParquetRecordReader[InternalRow](new CatalystReadSupport())

  private val row = new RowWrapper(config.inputFormatConfig.frameSchema)

  private var currentSourceId: CFVertexId = null
  private var currentEdge: DefaultEdge[CFVertexId, EdgeData4CFWritable] = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    context.getConfiguration.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentEdge: Edge[CFVertexId, EdgeData4CFWritable] = {
    currentEdge
  }

  override def getCurrentSourceId: CFVertexId = {
    currentSourceId
  }

  override def nextEdge(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val userId = new CFVertexId(row.stringValue(config.userColName), true)
      val itemId = new CFVertexId(row.stringValue(config.itemColName), false)
      val edgeData = new EdgeData4CFWritable()
      edgeData.setWeight(row.doubleValue(config.ratingColName))
      edgeData.setType(EdgeType.TRAIN)

      currentSourceId = userId
      currentEdge = new DefaultEdge[CFVertexId, EdgeData4CFWritable]()
      currentEdge.setTargetVertexId(itemId)
      currentEdge.setValue(edgeData)
    }

    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }

}
