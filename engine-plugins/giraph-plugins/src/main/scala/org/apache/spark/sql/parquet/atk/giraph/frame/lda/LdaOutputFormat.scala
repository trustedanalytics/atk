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


package org.apache.spark.sql.parquet.atk.giraph.frame.lda

import org.apache.giraph.graph.Vertex
import org.apache.giraph.io.{ VertexOutputFormat, VertexWriter }
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.spark.sql.catalyst.expressions.{ GenericRow, Row }
import org.apache.spark.sql.parquet.RowWriteSupport
import org.apache.spark.sql.parquet.atk.giraph.frame.MultiOutputCommitter
import org.apache.spark.sql.types._
import org.trustedanalytics.atk.giraph.config.lda.GiraphLdaConfiguration
import org.trustedanalytics.atk.giraph.io.{ LdaVertexData, LdaVertexId }
import parquet.hadoop.ParquetOutputFormat

/**
 * OutputFormat for LDA writes Vertices to two Parquet Frames
 */
class LdaParquetFrameVertexOutputFormat extends VertexOutputFormat[LdaVertexId, LdaVertexData, Nothing] {

  private val docResultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)
  private val wordResultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)
  private val topicResultsOutputFormat = new ParquetOutputFormat[Row](new RowWriteSupport)

  override def createVertexWriter(context: TaskAttemptContext): LdaParquetFrameVertexWriter = {
    new LdaParquetFrameVertexWriter(
      new GiraphLdaConfiguration(context.getConfiguration),
      docResultsOutputFormat,
      wordResultsOutputFormat,
      topicResultsOutputFormat
    )
  }

  override def checkOutputSpecs(context: JobContext): Unit = {
    new GiraphLdaConfiguration(context.getConfiguration).validate()
  }

  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    val outputFormatConfig = new GiraphLdaConfiguration(context.getConfiguration).ldaConfig.outputFormatConfig

    // configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.documentResultsFileLocation)
    val docCommitter = docResultsOutputFormat.getOutputCommitter(context)

    // re-configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.wordResultsFileLocation)
    val wordCommitter = wordResultsOutputFormat.getOutputCommitter(context)

    // re-configure outputdir for committer
    context.getConfiguration.set("mapreduce.output.fileoutputformat.outputdir", outputFormatConfig.topicResultsFileLocation)
    val topicCommitter = topicResultsOutputFormat.getOutputCommitter(context)

    new MultiOutputCommitter(List(docCommitter, wordCommitter, topicCommitter))
  }
}

object LdaOutputFormat {

  //Using JSON format for schema due to bug in Spark 1.3.0 which causes failures when reading StructType literal strings
  val LdaOutputRowSchema = StructType(
    StructField("id", StringType, nullable = false) ::
      StructField("result", ArrayType(DoubleType), nullable = true) :: Nil).json

}

class LdaParquetFrameVertexWriter(conf: GiraphLdaConfiguration,
                                  docResultsOutputFormat: ParquetOutputFormat[Row],
                                  wordResultsOutputFormat: ParquetOutputFormat[Row],
                                  topicResultsOutputFormat: ParquetOutputFormat[Row]) extends VertexWriter[LdaVertexId, LdaVertexData, Nothing] {

  private val outputFormatConfig = conf.ldaConfig.outputFormatConfig

  private var documentResultsWriter: RecordWriter[Void, Row] = null
  private var wordResultsWriter: RecordWriter[Void, Row] = null
  private var topicResultsWriter: RecordWriter[Void, Row] = null

  override def initialize(context: TaskAttemptContext): Unit = {
    // TODO: this looks like it will be needed in future version
    //context.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)
    val fileName = s"/part-${context.getTaskAttemptID.getTaskID.getId}.parquet"
    context.getConfiguration.set(RowWriteSupport.SPARK_ROW_SCHEMA, LdaOutputFormat.LdaOutputRowSchema)

    documentResultsWriter = docResultsOutputFormat.getRecordWriter(context,
      new Path(outputFormatConfig.documentResultsFileLocation + fileName))
    wordResultsWriter = wordResultsOutputFormat.getRecordWriter(context,
      new Path(outputFormatConfig.wordResultsFileLocation + fileName))
    topicResultsWriter = topicResultsOutputFormat.getRecordWriter(context,
      new Path(outputFormatConfig.topicResultsFileLocation + fileName))
  }

  override def close(context: TaskAttemptContext): Unit = {
    documentResultsWriter.close(context)
    wordResultsWriter.close(context)
    topicResultsWriter.close(context)
  }

  override def writeVertex(vertex: Vertex[LdaVertexId, LdaVertexData, Nothing]): Unit = {

    if (vertex.getId.isDocument) {
      documentResultsWriter.write(null, giraphLdaResultToRow(vertex))
    }
    else {
      wordResultsWriter.write(null, giraphLdaResultToRow(vertex))
      topicResultsWriter.write(null, giraphTopicGivenWordToRow(vertex))
    }
  }

  private def giraphLdaResultToRow(vertex: Vertex[LdaVertexId, LdaVertexData, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getValue.getOriginalId
    content(1) = vertex.getValue.getLdaResultAsDoubleArray.toSeq
    new GenericRow(content)
  }

  private def giraphTopicGivenWordToRow(vertex: Vertex[LdaVertexId, LdaVertexData, Nothing]): Row = {
    val content = new Array[Any](2)
    content(0) = vertex.getValue.getOriginalId
    content(1) = vertex.getValue.getTopicGivenWordAsDoubleArray.toSeq
    new GenericRow(content)
  }
}
