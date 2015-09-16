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
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce._
import org.apache.spark.mllib.atk.plugins.VectorUtils
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, Row }
import org.apache.spark.sql.parquet.RowWriteSupport
import org.apache.spark.sql.types._
import parquet.hadoop.ParquetOutputFormat

object LoopyBeliefPropagationOutputFormat {

  //Using JSON format for schema due to bug in Spark 1.3.0
  //which causes failures when reading StructType literal strings
  val OutputRowSchema = StructType(
    StructField("id", LongType, nullable = false) ::
      StructField("result", ArrayType(DoubleType), nullable = true) :: Nil).json
}
/**
 * OutputFormat for parquet frame
 */
class LoopyBeliefPropagationVertexOutputFormat extends VertexOutputFormat[LongWritable, VertexData4LBPWritable, Nothing] {

  private val resultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)

  /**
   * Creates a parquet vertex writer
   * @param context the execution context
   * @return the vertex writer
   */
  override def createVertexWriter(context: TaskAttemptContext): LoopyBeliefPropagationVertexWriter = {
    new LoopyBeliefPropagationVertexWriter(new LoopyBeliefPropagationConfiguration(context.getConfiguration), resultsOutputFormat)
  }

  /**
   * Validates the internal configuration data
   * @param context
   */
  override def checkOutputSpecs(context: JobContext): Unit = {
    new LoopyBeliefPropagationConfiguration(context.getConfiguration).validate()
  }

  /**
   * See parquet documentation
   * @param context execution context
   * @return output commiter
   */
  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new LoopyBeliefPropagationConfiguration(context.getConfiguration).getConfig.outputFormatConfig

    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.parquetFileLocation)
    resultsOutputFormat.getOutputCommitter(context)
  }
}

/**
 * Vertex writer class.
 * @param conf execution context
 * @param resultsOutputFormat output format for parquet
 */
class LoopyBeliefPropagationVertexWriter(conf: LoopyBeliefPropagationConfiguration,
                                         resultsOutputFormat: ParquetOutputFormat[Row])
    extends VertexWriter[LongWritable, VertexData4LBPWritable, Nothing] {

  private val outputFormatConfig = conf.getConfig.outputFormatConfig
  private var resultsWriter: RecordWriter[Void, Row] = null

  /**
   * initialize the writer
   * @param context execution context
   */
  override def initialize(context: TaskAttemptContext): Unit = {
    // TODO: this looks like it will be needed in future version
    //context.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, LoopyBeliefPropagationOutputFormat.OutputRowSchema)

    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    resultsWriter = resultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.parquetFileLocation + fileName))
  }

  /**
   * close the writer
   * @param context execution context
   */
  override def close(context: TaskAttemptContext): Unit = {
    resultsWriter.close(context)
  }

  /**
   * writer vertex to parquet
   * @param vertex vertex to write
   */
  override def writeVertex(vertex: Vertex[LongWritable, VertexData4LBPWritable, Nothing]): Unit = {

    resultsWriter.write(null, giraphVertexToRow(vertex))
  }

  private def giraphVertexToRow(vertex: Vertex[LongWritable, VertexData4LBPWritable, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getId.get()
    content(1) = VectorUtils.toScalaVector(vertex.getValue.getPosteriorVector())
    new GenericRow(content)
  }
}
