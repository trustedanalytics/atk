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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration
import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.spark.mllib.atk.plugins.VectorUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.execution.datasources.parquet.RowWriteSupport
import org.apache.spark.sql.execution.datasources.parquet.atk.giraph.frame.MultiOutputCommitter
import org.apache.spark.sql.types._
import org.trustedanalytics.atk.giraph.config.cf.CollaborativeFilteringConfiguration
import org.trustedanalytics.atk.giraph.io.{ CFVertexId, VertexData4CFWritable }
import parquet.hadoop.ParquetOutputFormat

/**
 * OutputFormat for LDA writes Vertices to two Parquet Frames
 */
class CollaborativeFilteringVertexOutputFormat[T <: VertexData4CFWritable] extends VertexOutputFormat[CFVertexId, T, Nothing] {

  private val userOutputFormat = new ParquetOutputFormat[InternalRow]()
  private val itemOutputFormat = new ParquetOutputFormat[InternalRow]()

  override def createVertexWriter(context: TaskAttemptContext): CollaborativeFilteringVertexWriter[T] = {
    new CollaborativeFilteringVertexWriter[T](new CollaborativeFilteringConfiguration(context.getConfiguration),
      userOutputFormat,
      itemOutputFormat)
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    new CollaborativeFilteringConfiguration(context.getConfiguration).validate()
  }

  override def setConf(conf: ImmutableClassesGiraphConfiguration[CFVertexId, T, Nothing]): Unit = {
    super.setConf(conf)
    conf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, classOf[RowWriteSupport].getName)
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new CollaborativeFilteringConfiguration(context.getConfiguration).getConfig.outputFormatConfig

    // configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.userFileLocation)
    val userCommitter = userOutputFormat.getOutputCommitter(context)

    // re-configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.itemFileLocation)
    val itemCommitter = itemOutputFormat.getOutputCommitter(context)

    new MultiOutputCommitter(List(userCommitter, itemCommitter))
  }
}

object CollaborativeFilteringOutputFormat {

  //Using JSON format for schema due to bug in Spark 1.3.0 which causes failures when reading StructType literal strings
  val OutputRowSchema = StructType(
    StructField("id", StringType, nullable = false) ::
      StructField("result", ArrayType(DoubleType), nullable = true) :: Nil).json
}

class CollaborativeFilteringVertexWriter[T <: VertexData4CFWritable](conf: CollaborativeFilteringConfiguration,
                                                                     userResultsOutputFormat: ParquetOutputFormat[InternalRow],
                                                                     itemResultsOutputFormat: ParquetOutputFormat[InternalRow])
    extends VertexWriter[CFVertexId, T, Nothing] {

  private val outputFormatConfig = conf.getConfig.outputFormatConfig

  private var userResultsWriter: RecordWriter[Void, InternalRow] = null
  private var itemResultsWriter: RecordWriter[Void, InternalRow] = null

  override def initialize(context: TaskAttemptContext): Unit = {
    context.getConfiguration.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, classOf[RowWriteSupport].getName)
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, CollaborativeFilteringOutputFormat.OutputRowSchema)

    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    userResultsWriter = userResultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.userFileLocation + fileName))
    itemResultsWriter = itemResultsOutputFormat.getRecordWriter(context, new Path(outputFormatConfig.itemFileLocation + fileName))
  }

  override def close(context: TaskAttemptContext): Unit = {
    userResultsWriter.close(context)
    itemResultsWriter.close(context)
  }

  override def writeVertex(vertex: Vertex[CFVertexId, T, Nothing]): Unit = {

    if (vertex.getId.isUser) {
      userResultsWriter.write(null, giraphVertexToRow(vertex))
    }
    else {
      itemResultsWriter.write(null, giraphVertexToRow(vertex))
    }
  }

  private def giraphVertexToRow(vertex: Vertex[CFVertexId, T, Nothing]): InternalRow = {
    val content = new Array[Any](2)
    content(0) = vertex.getId.getValue
    content(1) = VectorUtils.toDoubleArray(vertex.getValue.getVector).toSeq

    new GenericMutableRow(content)
  }
}
