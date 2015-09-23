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

package org.trustedanalytics.atk.giraph.plugins.model.lda

import org.trustedanalytics.atk.domain.frame.{ CovarianceMatrixArgs, FrameEntity }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.giraph.algorithms.lda.CVB0LDAComputation
import org.trustedanalytics.atk.giraph.algorithms.lda.CVB0LDAComputation.{ CVB0LDAAggregatorWriter, CVB0LDAMasterCompute }
import org.trustedanalytics.atk.giraph.config.lda._
import org.trustedanalytics.atk.giraph.io.{ LdaVertexId, LdaEdgeData, BigDataEdges }
import org.trustedanalytics.atk.giraph.plugins.util.{ GiraphConfigurationUtil, GiraphJobManager }
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.schema.{ Schema, DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.plugin._
import org.apache.spark.sql.parquet.atk.giraph.frame.lda.{ LdaVertexValueInputFormat, LdaParquetFrameVertexOutputFormat, LdaParquetFrameEdgeInputFormat }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.SparkContext._

import spray.json._
import LdaJsonFormat._

/**
 * Train plugin for Latent Dirichlet Allocation
 */
@PluginDoc(oneLine = "Creates Latent Dirichlet Allocation model",
  extended = """See the discussion about `Latent Dirichlet Allocation at Wikipedia. <http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation>`__""",
  returns = """dict
    The data returned is composed of multiple components:
topics_given_doc : Frame
    Frame with conditional probabilities of topic given document.
word_given_topics : Frame
    Frame with conditional probabilities of word given topic.
topics_given_word : Frame
    Frame with conditional probabilities of topic given word.
report : str
   The configuration and learning curve report for Latent Dirichlet
   Allocation as a multiple line str.""")
class LdaTrainPlugin
    extends SparkCommandPlugin[LdaTrainArgs, LdaTrainResult] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lda/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def execute(arguments: LdaTrainArgs)(implicit invocation: Invocation): LdaTrainResult = {

    val frames = engine.frames
    val config = ConfigFactory.load(this.getClass.getClassLoader)

    // validate arguments
    val edgeFrame: SparkFrame = arguments.frame
    edgeFrame.schema.requireColumnIsType(arguments.documentColumnName, DataTypes.string)
    edgeFrame.schema.requireColumnIsType(arguments.wordColumnName, DataTypes.string)
    edgeFrame.schema.requireColumnIsType(arguments.wordCountColumnName, DataTypes.isIntegerDataType)
    require(edgeFrame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(EngineConfig.config, "trustedanalytics.atk.engine.giraph")
    val giraphConf = new LdaConfiguration(hConf)

    val docOut = frames.create(CreateEntityArgs(description = Some("LDA doc results")))
    val wordOut = frames.create(CreateEntityArgs(description = Some("LDA word results")))
    val topicOut = frames.create(CreateEntityArgs(description = Some("LDA topics given word results")))

    val docOutSaveInfo = frames.prepareForSave(docOut)
    val wordOutSaveInfo = frames.prepareForSave(wordOut)
    val topicOutSaveInfo = frames.prepareForSave(topicOut)

    // assign unique long vertex Ids to vertices
    val vertexInputConfig = new LdaVertexInputFormatConfig(arguments)
    edgeFrame.rdd.cache()
    val docVertexFrame = createVertexFrame(edgeFrame.rdd, arguments.documentColumnName, 1, vertexInputConfig)
    val wordVertexFrame = createVertexFrame(edgeFrame.rdd, arguments.wordColumnName, 0, vertexInputConfig)
    //TODO : Remove forward refs
    var edgeFrameWithIds = joinFramesById(edgeFrame.rdd, docVertexFrame, arguments.documentColumnName, vertexInputConfig.vertexOriginalIdColumnName, vertexInputConfig.documentIdColumnName)
    edgeFrameWithIds = joinFramesById(edgeFrameWithIds, wordVertexFrame, arguments.wordColumnName, vertexInputConfig.vertexOriginalIdColumnName, vertexInputConfig.wordIdColumnName)
    val vertexValueFrame = createVertexValueFrame(docVertexFrame, wordVertexFrame, vertexInputConfig)

    val newEdgeFrame = engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("LDA edge frame with auto-assigned Ids"))) {
      frame: FrameEntity => frame.save(edgeFrameWithIds)
    }

    val newVertexFrame = engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("LDA vertex frame with auto-assigned Ids"))) {
      frame: FrameEntity => frame.save(vertexValueFrame)
    }
    val inputFormatConfig = new LdaInputFormatConfig(
      newEdgeFrame.getStorageLocation,
      newEdgeFrame.schema,
      newVertexFrame.getStorageLocation,
      newVertexFrame.schema
    )

    val outputFormatConfig = new LdaOutputFormatConfig(
      docOutSaveInfo.targetPath,
      wordOutSaveInfo.targetPath,
      topicOutSaveInfo.targetPath
    )
    val ldaConfig = new LdaConfig(inputFormatConfig, outputFormatConfig, arguments, vertexInputConfig)

    giraphConf.setLdaConfig(ldaConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)
    GiraphConfigurationUtil.set(giraphConf, "mapreduce.input.fileinputformat.inputdir", Some(inputFormatConfig.parquetEdgeFrameLocation))

    giraphConf.setEdgeInputFormatClass(classOf[LdaParquetFrameEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LdaParquetFrameVertexOutputFormat])
    giraphConf.setVertexInputFormatClass(classOf[LdaVertexValueInputFormat])
    giraphConf.setMasterComputeClass(classOf[CVB0LDAMasterCompute])
    giraphConf.setComputationClass(classOf[CVB0LDAComputation])
    giraphConf.setAggregatorWriterClass(classOf[CVB0LDAAggregatorWriter])

    //Enable only if serialized edges for single vertex exceed 1GB
    if (config.getBoolean("trustedanalytics.atk.lda-model-train.useBigDataEdges")) {
      giraphConf.setOutEdgesClass(classOf[BigDataEdges[LdaVertexId, LdaEdgeData]])
    }

    val report = GiraphJobManager.run(s"ia_giraph_lda_train_${invocation.asInstanceOf[CommandInvocation].commandId}",
      classOf[CVB0LDAComputation].getCanonicalName,
      giraphConf,
      invocation,
      "lda-learning-report_0")

    val resultsColumnName = "topic_probabilities"
    val resultsColumn = Column(resultsColumnName, DataTypes.vector(arguments.getNumTopics))

    // After saving update timestamps, status, row count, etc.
    frames.postSave(docOut, docOutSaveInfo, new FrameSchema(List(edgeFrame.schema.column(arguments.documentColumnName), resultsColumn)))
    frames.postSave(wordOut, wordOutSaveInfo, new FrameSchema(List(edgeFrame.schema.column(arguments.wordColumnName), resultsColumn)))
    val topicFrame = frames.postSave(topicOut, topicOutSaveInfo, new FrameSchema(List(edgeFrame.schema.column(arguments.wordColumnName), resultsColumn)))

    val model: Model = arguments.model

    model.data = LdaModel.createLdaModel(frames.getAllRows(topicFrame),
      topicFrame.schema,
      arguments.wordColumnName,
      resultsColumnName,
      arguments.getNumTopics
    ).toJson.asJsObject

    LdaTrainResult(
      frames.expectFrame(docOut.toReference),
      frames.expectFrame(wordOut.toReference),
      frames.expectFrame(topicOut.toReference),
      report)
  }

  /**
   * Assign a unique long ID to vertices and output frame
   *
   * @param frameRdd Input frame
   * @param columnName Input column
   * @return Frame with IDs assigned
   */
  def createVertexFrame(frameRdd: FrameRdd, columnName: String, isDocument: Int, config: LdaVertexInputFormatConfig): FrameRdd = {
    val longIdColumnName = if (isDocument == 1) config.documentIdColumnName else config.wordIdColumnName
    val vertexFrameSchema = FrameSchema(List(
      Column(longIdColumnName, DataTypes.int64),
      Column(config.vertexOriginalIdColumnName, DataTypes.string),
      Column(config.isDocumentColumnName, DataTypes.int32)))

    val idAssigner = new LdaGraphIDAssigner()

    val uniqueVertices = frameRdd.mapRows(row => {
      row.stringValue(columnName)
    }).distinct()

    val vertexRdd: RDD[Row] = idAssigner.assignVertexId(uniqueVertices).map {
      case (longId, stringId) =>
        new GenericRow(Array[Any](longId, stringId, isDocument))
    }
    new FrameRdd(vertexFrameSchema, vertexRdd)
  }

  def joinFramesById(edgeFrame: FrameRdd,
                     vertexFrame: FrameRdd,
                     edgeJoinColumnName: String,
                     vertexJoinColumnName: String,
                     vertexIdColumnName: String): FrameRdd = {
    val leftRdd = edgeFrame.keyByRows(row => row.value(edgeJoinColumnName))
    val rightRdd = vertexFrame.mapRows(row => (row.value(vertexJoinColumnName), row.longValue(vertexIdColumnName)))
    val joinedRdd: RDD[Row] = leftRdd.join(rightRdd).map {
      case (key, (leftRow, id)) => Row.fromSeq(leftRow.toSeq :+ id)
    }

    val newSchema = FrameSchema(Schema.join(edgeFrame.frameSchema.columns, List(Column(vertexIdColumnName, DataTypes.int64))))
    new FrameRdd(newSchema, joinedRdd)
  }

  def createVertexValueFrame(docVertexFrame: RDD[Row], wordVertexFrame: RDD[Row], config: LdaVertexInputFormatConfig): FrameRdd = {
    val vertexFrameSchema = FrameSchema(List(
      Column(config.vertexIdColumnName, DataTypes.int64),
      Column(config.vertexOriginalIdColumnName, DataTypes.string),
      Column(config.isDocumentColumnName, DataTypes.int32)))

    new FrameRdd(vertexFrameSchema, docVertexFrame.union(wordVertexFrame))
  }

}
