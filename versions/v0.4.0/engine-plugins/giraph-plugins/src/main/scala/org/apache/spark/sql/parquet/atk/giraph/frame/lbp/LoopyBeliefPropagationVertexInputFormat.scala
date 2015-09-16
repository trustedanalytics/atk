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

package org.apache.spark.sql.parquet.atk.giraph.frame.lbp

import org.trustedanalytics.atk.giraph.io.VertexData4LBPWritable
import org.trustedanalytics.atk.giraph.config.lbp.LoopyBeliefPropagationConfiguration
import org.trustedanalytics.atk.engine.frame.RowWrapper
import org.apache.giraph.io.{ VertexValueReader, VertexValueInputFormat }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, JobContext }
import org.apache.mahout.math.DenseVector
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.parquet.RowReadSupport
import parquet.hadoop.{ ParquetRecordReader, ParquetInputFormat }

import scala.collection.JavaConverters._

/**
 * Vertex input format class.
 */
class LoopyBeliefPropagationVertexInputFormat extends VertexValueInputFormat[LongWritable, VertexData4LBPWritable] {

  private val parquetInputFormat = new ParquetInputFormat[Row](classOf[RowReadSupport])

  /**
   * Validate the input parameters
   * @param conf giraph configuration
   */
  override def checkInputSpecs(conf: Configuration): Unit = {
    new LoopyBeliefPropagationConfiguration(conf).validate()
  }

  /**
   * Creates a vertex reader for giraph engine
   * @param split data split
   * @param context execution context
   * @return vertex reader
   */
  override def createVertexValueReader(split: InputSplit, context: TaskAttemptContext): VertexValueReader[LongWritable, VertexData4LBPWritable] = {
    new LoopyBeliefPropagationVertexReader(new LoopyBeliefPropagationConfiguration(context.getConfiguration), parquetInputFormat)
  }

  override def getSplits(context: JobContext, minSplitCountHint: Int): java.util.List[InputSplit] = {
    parquetInputFormat.getSplits(context)
  }
}

/**
 * Vertex reader class for parquet
 * @param conf reader configuration
 * @param vertexInputFormat format for vertex reader
 */
class LoopyBeliefPropagationVertexReader(conf: LoopyBeliefPropagationConfiguration, vertexInputFormat: ParquetInputFormat[Row])
    extends VertexValueReader[LongWritable, VertexData4LBPWritable] {

  private val config = conf.getConfig
  private val reader = new ParquetRecordReader[Row](new RowReadSupport)
  private val row = new RowWrapper(config.inputFormatConfig.frameSchema)
  private var currentVertexId: LongWritable = null
  private var currentVertexValue: VertexData4LBPWritable = null

  /**
   * initialize the reader
   * @param split data split
   * @param context execution context
   */
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    reader.initialize(split, context)
  }

  /**
   * Close the reader
   */
  override def close(): Unit = {
    reader.close()
  }

  /**
   * Get the next vertex from parquet
   * @return true if a new vertex has been read; false otherwise
   */
  override def nextVertex(): Boolean = {
    val hasNext: Boolean = reader.nextKeyValue

    if (hasNext) {
      row.apply(reader.getCurrentValue)
      currentVertexId = new LongWritable(row.longValue(config.srcColName))
      val values = row.vectorValue(config.srcLabelColName)
      val denseVector = new DenseVector(values.toArray)
      val vertexType = if (config.ignoreVertexType) VertexData4LBPWritable.VertexType.TRAIN else VertexData4LBPWritable.VertexType.VALIDATE

      currentVertexValue = new VertexData4LBPWritable(vertexType, denseVector, denseVector)
    }

    hasNext
  }

  /**
   * See parquet documentation for the progress indicator
   * @return see documentation
   */
  override def getProgress: Float = {
    reader.getProgress
  }

  override def getCurrentVertexId: LongWritable = {
    currentVertexId
  }

  override def getCurrentVertexValue: VertexData4LBPWritable = {
    currentVertexValue
  }
}

