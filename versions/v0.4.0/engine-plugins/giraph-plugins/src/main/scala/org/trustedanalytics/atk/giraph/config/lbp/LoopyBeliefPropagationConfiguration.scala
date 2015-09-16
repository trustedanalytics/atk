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

package org.trustedanalytics.atk.giraph.config.lbp

import org.trustedanalytics.atk.domain.schema.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.hadoop.conf.Configuration
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Config for Input
 * @param parquetFileLocation parquet input frame
 */
case class LoopyBeliefPropagationInputFormatConfig(parquetFileLocation: String,
                                                   frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for Output
 * @param parquetFileLocation parquet output frame file location in HDFS
 */
case class LoopyBeliefPropagationOutputFormatConfig(parquetFileLocation: String) {
  require(StringUtils.isNotBlank(parquetFileLocation), "output file location is required")
}

/**
 *
 * @param inputFormatConfig input configuration
 * @param outputFormatConfig output configuration
 * @param srcColName column name for the source vertex
 * @param destColName column name for the dest vertex
 * @param weightColName column name for the edge weight
 * @param srcLabelColName column name for the source labels
 * @param resultColName column name for the results column (calculated by the algorithm)
 * @param maxIterations max number of iterations for the algorithm
 * @param convergenceThreshold deprecated - do not use
 * @param anchorThreshold
 * @param smoothing
 * @param maxProduct
 * @param power
 */
case class LoopyBeliefPropagationConfig(inputFormatConfig: LoopyBeliefPropagationInputFormatConfig,
                                        outputFormatConfig: LoopyBeliefPropagationOutputFormatConfig,
                                        srcColName: String,
                                        destColName: String,
                                        weightColName: String,
                                        srcLabelColName: String,
                                        resultColName: String,
                                        ignoreVertexType: Boolean,
                                        maxIterations: Int,
                                        convergenceThreshold: Float,
                                        anchorThreshold: Double,
                                        smoothing: Float,
                                        maxProduct: Boolean,
                                        power: Float) {

  def this(inputFormatConfig: LoopyBeliefPropagationInputFormatConfig,
           outputFormatConfig: LoopyBeliefPropagationOutputFormatConfig,
           args: LoopyBeliefPropagationArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.srcColName,
      args.destColName,
      args.weightColName,
      args.srcLabelColName,
      args.getResultsColName,
      args.getIgnoreVertexType,
      args.getMaxIterations,
      args.getConvergenceThreshold,
      args.getAnchorThreshold,
      args.getSmoothing,
      args.getMaxProduct,
      args.getPower)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
}

/**
 * JSON formats.
 */
object LoopyBeliefPropagationConfigJSONFormat {
  implicit val inputFormatConfigFormat = jsonFormat2(LoopyBeliefPropagationInputFormatConfig)
  implicit val outputFormatConfigFormat = jsonFormat1(LoopyBeliefPropagationOutputFormatConfig)
  implicit val configFormat = jsonFormat14(LoopyBeliefPropagationConfig)
}

import LoopyBeliefPropagationConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class LoopyBeliefPropagationConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val ConfigPropertyName = "loopyBeliefPropagation.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(ConfigPropertyName) != null, "loopyBeliefPropagation.config property was not set in the Configuration")
  }

  def getConfig: LoopyBeliefPropagationConfig = {
    JsonParser(get(ConfigPropertyName)).asJsObject.convertTo[LoopyBeliefPropagationConfig]
  }

  def setConfig(value: LoopyBeliefPropagationConfig): Unit = {
    set(ConfigPropertyName, value.toJson.compactPrint)
  }
}
