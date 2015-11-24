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

package org.trustedanalytics.atk.giraph.config.lp

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
case class LabelPropagationInputFormatConfig(parquetFileLocation: String,
                                             frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for Output
 * @param parquetFileLocation parquet output frame file location in HDFS
 */
case class LabelPropagationOutputFormatConfig(parquetFileLocation: String) {
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
 * @param lambda deprecated - do not use
 */
case class LabelPropagationConfig(inputFormatConfig: LabelPropagationInputFormatConfig,
                                  outputFormatConfig: LabelPropagationOutputFormatConfig,
                                  srcColName: String,
                                  destColName: String,
                                  weightColName: String,
                                  srcLabelColName: String,
                                  resultColName: String,
                                  maxIterations: Int,
                                  convergenceThreshold: Float,
                                  lambda: Float) {

  def this(inputFormatConfig: LabelPropagationInputFormatConfig,
           outputFormatConfig: LabelPropagationOutputFormatConfig,
           args: LabelPropagationArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.srcColName,
      args.destColName,
      args.weightColName,
      args.srcLabelColName,
      args.getResultsColName,
      args.getMaxIterations,
      args.getConvergenceThreshold,
      args.getLambda)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
}

/**
 * JSON formats.
 */
object LabelPropagationConfigJSONFormat {
  implicit val inputFormatConfigFormat = jsonFormat2(LabelPropagationInputFormatConfig)
  implicit val outputFormatConfigFormat = jsonFormat1(LabelPropagationOutputFormatConfig)
  implicit val configFormat = jsonFormat10(LabelPropagationConfig)
}

import LabelPropagationConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class LabelPropagationConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val ConfigPropertyName = "labelPropagation.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(ConfigPropertyName) != null, "labelPropagation.config property was not set in the Configuration")
  }

  def getConfig: LabelPropagationConfig = {
    JsonParser(get(ConfigPropertyName)).asJsObject.convertTo[LabelPropagationConfig]
  }

  def setConfig(value: LabelPropagationConfig): Unit = {
    set(ConfigPropertyName, value.toJson.compactPrint)
  }
}
