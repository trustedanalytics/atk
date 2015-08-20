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

package org.trustedanalytics.atk.giraph.plugins.frame

import org.apache.spark.sql.parquet.atk.giraph.frame.lp.{ LabelPropagationEdgeInputFormat, LabelPropagationVertexOutputFormat, LabelPropagationVertexInputFormat }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.giraph.algorithms.lp.LabelPropagationComputation
import org.trustedanalytics.atk.giraph.algorithms.lp.LabelPropagationComputation.{ LabelPropagationAggregatorWriter, LabelPropagationMasterCompute }
import org.trustedanalytics.atk.giraph.config.lp._
import org.trustedanalytics.atk.giraph.plugins.util.{ GiraphConfigurationUtil, GiraphJobManager }
import org.trustedanalytics.atk.domain.{ StorageFormats, CreateEntityArgs }
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }
import LabelPropagationJsonFormat._

@PluginDoc(oneLine = "Loopy Belief Propagation (LBP) from http://en.wikipedia.org/wiki/Belief_propagation",
  extended = """Label Propagation on Gaussian Random Fields.
               |
               |This algorithm is presented in X. Zhu and Z. Ghahramani.
               |Learning from labeled and unlabeled data with label propagation.
               |Technical Report CMU-CALD-02-107, CMU, 2002.
               |
               |
               |Parameters
               |----------
               |src_col_name: str
               |    The column name for the source vertex id.
               |dest_col_name: str
               |    The column name for the destination vertex id.
               |weight_col_name: str
               |    The column name for the edge weight.
               |src_label_col_name: str
               |    The column name for the label properties for the source vertex.
               |result_col_name : str (optional)
               |    column name for the results (holding the post labels for the vertices)
               |max_iterations : int (optional)
               |    The maximum number of supersteps that the algorithm will execute.
               |    The valid value range is all positive int.
               |    The default value is 10.
               |convergence_threshold : float (optional)
               |    The amount of change in cost function that will be tolerated at
               |    convergence.
               |    If the change is less than this threshold, the algorithm exits earlier
               |    before it reaches the maximum number of supersteps.
               |    The valid value range is all float and zero.
               |    The default value is 0.00000001f.
               |alpha : float (optional)
               |    The tradeoff parameter that controls how much influence an external
               |    classifier's prediction contributes to the final prediction.
               |    This is for the case where an external classifier is available that can
               |    produce initial probabilistic classification on unlabeled examples, and
               |    the option allows incorporating external classifier's prediction into
               |    the LP training process.
               |    The valid value range is [0.0,1.0].
               |    The default value is 0.""",
  returns = """Returns
              |-------
              |a 2-column frame:
              |
              |vertex: int
              |    A vertex id.
              |result : Vector (long)
              |    label vector for the results (for the node id in column 1)""")
class LabelPropagationPlugin
    extends CommandPlugin[LabelPropagationArgs, LabelPropagationResult] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame:/label_propagation"

  override def execute(arguments: LabelPropagationArgs)(implicit context: Invocation): LabelPropagationResult = {

    val frames = engine.frames

    //TODO validate frame args here
    val frame = frames.expectFrame(arguments.frame)
    require(frame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    // setup and run
    val hadoopConf = GiraphConfigurationUtil.newHadoopConfigurationFrom(EngineConfig.config, "trustedanalytics.atk.engine.giraph")
    val giraphConf = new LabelPropagationConfiguration(hadoopConf)

    val resultsFrameRef = frames.create(CreateEntityArgs(description = Some("Label propagation results"))).toReference
    val resultsSaveInfo = frames.prepareForSave(resultsFrameRef, storageFormat = Some(StorageFormats.FileParquet))
    val inputFormatConfig = new LabelPropagationInputFormatConfig(frame.getStorageLocation, frame.schema)
    val outputFormatConfig = new LabelPropagationOutputFormatConfig(resultsSaveInfo.targetPath)
    val labelPropagationConfig = new LabelPropagationConfig(inputFormatConfig, outputFormatConfig, arguments)

    giraphConf.setConfig(labelPropagationConfig)
    GiraphConfigurationUtil.set(giraphConf, "giraphjob.maxSteps", arguments.maxIterations)
    GiraphConfigurationUtil.set(giraphConf, "mapreduce.input.fileinputformat.inputdir", Some(inputFormatConfig.parquetFileLocation))

    giraphConf.setEdgeInputFormatClass(classOf[LabelPropagationEdgeInputFormat])
    giraphConf.setVertexOutputFormatClass(classOf[LabelPropagationVertexOutputFormat])
    giraphConf.setVertexInputFormatClass(classOf[LabelPropagationVertexInputFormat])
    giraphConf.setMasterComputeClass(classOf[LabelPropagationMasterCompute])
    giraphConf.setComputationClass(classOf[LabelPropagationComputation])
    giraphConf.setAggregatorWriterClass(classOf[LabelPropagationAggregatorWriter])

    val result = GiraphJobManager.run("ia_giraph_lp",
      classOf[LabelPropagationComputation].getCanonicalName,
      giraphConf,
      context,
      "lp-learning-report_0")

    val resultsColumn = Column(arguments.srcLabelColName, frame.schema.columnDataType(arguments.srcLabelColName))
    frames.postSave(resultsFrameRef, resultsSaveInfo, new FrameSchema(List(frame.schema.column(arguments.srcColName), resultsColumn)))

    LabelPropagationResult(frames.expectFrame(resultsFrameRef), result)

  }

}
