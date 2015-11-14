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

package org.apache.spark.sql.execution.datasources.parquet.atk.giraph.frame.lp

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration
import org.apache.giraph.edge.{DefaultEdge, Edge}
import org.apache.giraph.io._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{Writable, DoubleWritable, LongWritable}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, TaskAttemptContext}
import org.apache.parquet.hadoop.{ParquetRecordReader, ParquetInputFormat}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.CatalystReadSupport
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.trustedanalytics.atk.giraph.config.lp.LabelPropagationConfiguration

/**
 * InputFormat for Label propagation reads edges from Parquet Frame
 */
class LabelPropagationEdgeInputFormat extends EdgeInputFormat[LongWritable, DoubleWritable] {

  private val parquetInputFormat = new ParquetInputFormat[InternalRow]()

  override def checkInputSpecs(conf: Configuration): Unit = {
    new LabelPropagationConfiguration(conf).validate()
  }


  override def setConf(conf: ImmutableClassesGiraphConfiguration[LongWritable, Writable, DoubleWritable]): Unit = {
    super.setConf(conf)
    conf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)
  }

  /**
   * Create edge reader for parquet
   * @param split data splits
   * @param context execution context
   * @return the edge reader
   */
  override def createEdgeReader(split: InputSplit, context: TaskAttemptContext): EdgeReader[LongWritable, DoubleWritable] = {
    val edgeReader = new LabelPropagationEdgeReader(new LabelPropagationConfiguration(context.getConfiguration))
    // algorithm expects edges that go both ways (seems to be how undirected is modeled in Giraph)
    new ReverseEdgeDuplicator[LongWritable, DoubleWritable](edgeReader)
  }

  /**
   * Get data splits
   * @param context execution context
   * @param minSplitCountHint number of desired splits
   * @return a list of input splits
   */
  override def getSplits(context: JobContext, minSplitCountHint: Int): java.util.List[InputSplit] = {
    parquetInputFormat.getSplits(context)
  }
}

/**
 * The edge reader class for parquet
 * @param config configuration data
 */
class LabelPropagationEdgeReader(config: LabelPropagationConfiguration)
  extends EdgeReader[LongWritable, DoubleWritable] {

  private val conf = config.getConfig
  private val reader = new ParquetRecordReader[InternalRow](new CatalystReadSupport())
  private val row = new RowWrapper(conf.inputFormatConfig.frameSchema)

  private var currentSourceId: LongWritable = null
  private var currentEdge: DefaultEdge[LongWritable, DoubleWritable] = null

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext): Unit = {
    context.getConfiguration.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[CatalystReadSupport].getName)
    reader.initialize(inputSplit, context)
  }

  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentEdge: Edge[LongWritable, DoubleWritable] = {
    currentEdge
  }

  override def getCurrentSourceId: LongWritable = {
    currentSourceId
  }

  override def nextEdge(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue
    if (hasNext) {
      row.apply(reader.getCurrentValue)

      val sourceId = new LongWritable(row.longValue(conf.srcColName))
      val destId = new LongWritable(row.longValue(conf.destColName))
      val edgeWeight = new DoubleWritable(row.stringValue(conf.weightColName).toDouble)

      currentSourceId = sourceId
      currentEdge = new DefaultEdge[LongWritable, DoubleWritable]()
      currentEdge.setTargetVertexId(destId)
      currentEdge.setValue(edgeWeight)
    }
    hasNext
  }

  override def close(): Unit = {
    reader.close()
  }
}
