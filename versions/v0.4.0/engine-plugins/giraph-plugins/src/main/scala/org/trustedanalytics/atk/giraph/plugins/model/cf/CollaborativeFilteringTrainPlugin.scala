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

package org.trustedanalytics.atk.giraph.plugins.model.cf

import org.apache.spark.sql.parquet.atk.giraph.frame.cf.{ CollaborativeFilteringEdgeInputFormat, CollaborativeFilteringVertexOutputFormat }
import org.trustedanalytics.atk.domain.{ StringValue, CreateEntityArgs }
import org.trustedanalytics.atk.domain.frame.FrameName
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }
import org.trustedanalytics.atk.giraph.algorithms.als.AlternatingLeastSquaresComputation
import org.trustedanalytics.atk.giraph.algorithms.als.AlternatingLeastSquaresComputation.{ AlternatingLeastSquaresAggregatorWriter, AlternatingLeastSquaresMasterCompute }
import org.trustedanalytics.atk.giraph.algorithms.cgd.ConjugateGradientDescentComputation
import org.trustedanalytics.atk.giraph.algorithms.cgd.ConjugateGradientDescentComputation.{ ConjugateGradientDescentAggregatorWriter, ConjugateGradientDescentMasterCompute }
import org.trustedanalytics.atk.giraph.config.cf._
import org.trustedanalytics.atk.giraph.io.{ VertexData4CFWritable, VertexData4CGDWritable }
import org.trustedanalytics.atk.giraph.plugins.util.{ GiraphConfigurationUtil, GiraphJobManager }

import spray.json._
import CollaborativeFilteringJsonFormat._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Collaborative filtering (als/cgd) model",
  extended = """""",
  returns = """Execution result summary for Giraph""")
class CollaborativeFilteringTrainPlugin
    extends CommandPlugin[CollaborativeFilteringTrainArgs, StringValue] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/train"

  override def execute(arguments: CollaborativeFilteringTrainArgs)(implicit context: Invocation): StringValue = {

    val frames = engine.frames
    val frame = frames.expectFrame(arguments.frame)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hadoopConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(EngineConfig.config, "trustedanalytics.atk.engine.giraph")
    val giraphConf = new CollaborativeFilteringConfiguration(hadoopConf)

    val userFrameName = FrameName.generate(Some("user_"))
    val itemFrameName = FrameName.generate(Some("item_"))
    val userFrame = frames.create(CreateEntityArgs(name = Some(userFrameName), description = Some("Collaborative filtering user frame results")))
    val itemFrame = frames.create(CreateEntityArgs(name = Some(itemFrameName), description = Some("Collaborative filtering item frame results")))
    val userFrameSaveInfo = frames.prepareForSave(userFrame)
    val itemFrameSaveInfo = frames.prepareForSave(itemFrame)
    val inputFormatConfig = new CollaborativeFilteringInputFormatConfig(frame.storageLocation.get, frame.schema)
    val outputFormatConfig = new CollaborativeFilteringOutputFormatConfig(userFrameSaveInfo.targetPath, itemFrameSaveInfo.targetPath)
    val collaborativeFilteringConfig = new CollaborativeFilteringConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setConfig(collaborativeFilteringConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)
    GiraphConfigurationUtil.set(giraphConf, "mapreduce.input.fileinputformat.inputdir", Some(inputFormatConfig.parquetFileLocation))

    giraphConf.setEdgeInputFormatClass(classOf[CollaborativeFilteringEdgeInputFormat])

    var computation: String = null
    if (CollaborativeFilteringConstants.alsAlgorithm.equalsIgnoreCase(collaborativeFilteringConfig.evaluationFunction)) {
      giraphConf.setVertexOutputFormatClass(classOf[CollaborativeFilteringVertexOutputFormat[VertexData4CFWritable]])
      giraphConf.setMasterComputeClass(classOf[AlternatingLeastSquaresMasterCompute])
      giraphConf.setComputationClass(classOf[AlternatingLeastSquaresComputation])
      giraphConf.setAggregatorWriterClass(classOf[AlternatingLeastSquaresAggregatorWriter])
      computation = classOf[AlternatingLeastSquaresComputation].getCanonicalName
    }
    else if (CollaborativeFilteringConstants.cgdAlgorithm.equalsIgnoreCase(collaborativeFilteringConfig.evaluationFunction)) {
      giraphConf.setVertexOutputFormatClass(classOf[CollaborativeFilteringVertexOutputFormat[VertexData4CGDWritable]])
      giraphConf.setMasterComputeClass(classOf[ConjugateGradientDescentMasterCompute])
      giraphConf.setComputationClass(classOf[ConjugateGradientDescentComputation])
      giraphConf.setAggregatorWriterClass(classOf[ConjugateGradientDescentAggregatorWriter])
      computation = classOf[ConjugateGradientDescentComputation].getCanonicalName
    }
    else {
      throw new NotImplementedError("only als & cgd algorithms are supported")
    }

    val result = GiraphJobManager.run("cf_giraph",
      computation,
      giraphConf,
      context,
      CollaborativeFilteringConstants.reportFilename)

    val factorsColumnName = "cf_factors"
    val resultsColumn = Column(factorsColumnName, DataTypes.vector(arguments.getNumFactors))
    frames.postSave(userFrame.toReference, userFrameSaveInfo, new FrameSchema(List(frame.schema.column(arguments.userColName), resultsColumn)))
    frames.postSave(itemFrame.toReference, itemFrameSaveInfo, new FrameSchema(List(frame.schema.column(arguments.itemColName), resultsColumn)))

    //Writing the model as JSON
    val jsonModel = new CollaborativeFilteringData(userFrameReference = userFrame.toReference,
      itemFrameReference = itemFrame.toReference,
      userColumnName = arguments.userColName,
      itemColumnName = arguments.itemColName,
      factorsColumnName = factorsColumnName,
      numFactors = arguments.getNumFactors)
    val modelData = arguments.model.data = jsonModel.toJson.asJsObject

    StringValue(result)
  }

}
