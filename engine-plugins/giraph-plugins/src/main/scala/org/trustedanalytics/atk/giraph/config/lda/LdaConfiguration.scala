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

package org.trustedanalytics.atk.giraph.config.lda

import org.trustedanalytics.atk.domain.schema.Schema
import org.apache.commons.lang3.StringUtils
import org.apache.giraph.conf.GiraphConfiguration
import org.apache.hadoop.conf.Configuration
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import LdaJsonFormat._

/**
 * Config for LDA Input
 * @param parquetFileLocation parquet input frame
 */
case class LdaInputFormatConfig(parquetFileLocation: String,
                                frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for LDA Output
 * @param documentResultsFileLocation parquet output frame file location in HDFS
 * @param wordResultsFileLocation parquet output frame file location in HDFS
 */
case class LdaOutputFormatConfig(documentResultsFileLocation: String, wordResultsFileLocation: String) {
  require(StringUtils.isNotBlank(documentResultsFileLocation), "document lda results file location is required")
  require(StringUtils.isNotBlank(wordResultsFileLocation), "word lda results file location is required")
}

/**
 * Configuration settings for Lda
 * @param inputFormatConfig where to read input from
 * @param outputFormatConfig where to write output to
 * @param documentColumnName column name that contains the "documents"
 * @param wordColumnName column name that contains the "words"
 * @param wordCountColumnName column name that contains "word count"
 * @param maxIterations see LdaTrainArgs for doc
 * @param alpha see LdaTrainArgs for doc
 * @param beta see LdaTrainArgs for doc
 * @param convergenceThreshold see LdaTrainArgs for doc
 * @param evaluationCost see LdaTrainArgs for doc
 * @param numTopics see LdaTrainArgs for doc
 */
case class LdaConfig(inputFormatConfig: LdaInputFormatConfig,
                     outputFormatConfig: LdaOutputFormatConfig,
                     documentColumnName: String,
                     wordColumnName: String,
                     wordCountColumnName: String,
                     maxIterations: Long,
                     alpha: Float,
                     beta: Float,
                     convergenceThreshold: Float,
                     evaluationCost: Boolean,
                     numTopics: Int) {

  def this(inputFormatConfig: LdaInputFormatConfig, outputFormatConfig: LdaOutputFormatConfig, args: LdaTrainArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.documentColumnName,
      args.wordColumnName,
      args.wordCountColumnName,
      args.getMaxIterations,
      args.getAlpha,
      args.getBeta,
      args.getConvergenceThreshold,
      args.getEvaluateCost,
      args.getNumTopics)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
  require(StringUtils.isNotBlank(documentColumnName), "document column name is required")
  require(StringUtils.isNotBlank(wordColumnName), "word column name is required")
  require(StringUtils.isNotBlank(wordCountColumnName), "word count column name is required")
  require(maxIterations > 0, "Max iterations should be greater than 0")
  require(alpha > 0, "Alpha should be greater than 0")
  require(beta > 0, "Beta should be greater than 0")
  require(numTopics > 0, "Number of topics (K) should be greater than 0")
}

/**
 * JSON formats needed by Lda.
 */
object LdaConfigJSONFormat {
  implicit val ldaInputFormatConfigFormat = jsonFormat2(LdaInputFormatConfig)
  implicit val ldaOutputFormatConfigFormat = jsonFormat2(LdaOutputFormatConfig)
  implicit val ldaConfigFormat = jsonFormat11(LdaConfig)
}

import LdaConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class LdaConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val LdaConfigPropertyName = "lda.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(LdaConfigPropertyName) != null, "lda.config property was not set in the Configuration")
  }

  def ldaConfig: LdaConfig = {
    // all of the settings can go into one JSON string so we don't need a bunch of String constants passed around
    JsonParser(get(LdaConfigPropertyName)).asJsObject.convertTo[LdaConfig]
  }

  def setLdaConfig(value: LdaConfig): Unit = {
    // all of the settings can go into one JSON string so we don't need a bunch of String constants passed around
    set(LdaConfigPropertyName, value.toJson.compactPrint)
  }
}
