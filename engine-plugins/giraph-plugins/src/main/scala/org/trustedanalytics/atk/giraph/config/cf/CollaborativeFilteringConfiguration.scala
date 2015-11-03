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


package org.trustedanalytics.atk.giraph.config.cf

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
case class CollaborativeFilteringInputFormatConfig(parquetFileLocation: String,
                                                   frameSchema: Schema) {
  require(StringUtils.isNotBlank(parquetFileLocation), "input file location is required")
  require(frameSchema != null, "input frame schema is required")
}

/**
 * Configuration for Output
 * @param userFileLocation parquet output frame file location in HDFS
 */
case class CollaborativeFilteringOutputFormatConfig(userFileLocation: String, itemFileLocation: String) {
  require(StringUtils.isNotBlank(userFileLocation), "user output file is required")
  require(StringUtils.isNotBlank(userFileLocation), "item output file is required")
}

/**
 *
 * @param inputFormatConfig input format configuration
 * @param outputFormatConfig output format configuration
 * @param userColName db user column name
 * @param itemColName db item column name
 * @param ratingColName db rating column name
 * @param evaluationFunction "alg" or "cgd"
 * @param numFactors factors for the output array
 * @param maxIterations mac iterations
 * @param convergenceThreshold convergence threshold
 * @param lambda lambda
 * @param biasOn alg bias
 * @param maxValue max value
 * @param minValue min value
 * @param learningCurveInterval learning curve interval
 */
case class CollaborativeFilteringConfig(inputFormatConfig: CollaborativeFilteringInputFormatConfig,
                                        outputFormatConfig: CollaborativeFilteringOutputFormatConfig,
                                        userColName: String,
                                        itemColName: String,
                                        ratingColName: String,
                                        evaluationFunction: String,
                                        numFactors: Int,
                                        maxIterations: Int,
                                        convergenceThreshold: Double,
                                        lambda: Float,
                                        biasOn: Boolean,
                                        minValue: Float,
                                        maxValue: Float,
                                        learningCurveInterval: Int,
                                        cgdIterations: Int,
                                        reportFilename: String) {

  def this(inputFormatConfig: CollaborativeFilteringInputFormatConfig,
           outputFormatConfig: CollaborativeFilteringOutputFormatConfig,
           args: CollaborativeFilteringTrainArgs) = {
    this(inputFormatConfig,
      outputFormatConfig,
      args.userColName,
      args.itemColName,
      args.ratingColName,
      args.getEvaluationFunction,
      args.getNumFactors,
      args.getMaxIterations,
      args.getConvergenceThreshold,
      args.getLambda,
      args.getBias,
      args.getMinValue,
      args.getMaxValue,
      args.getLearningCurveInterval,
      args.getCgdIterations,
      CollaborativeFilteringConstants.reportFilename)
  }
  require(inputFormatConfig != null, "input format is required")
  require(outputFormatConfig != null, "output format is required")
}

/**
 * JSON formats.
 */
object CollaborativeFilteringConfigJSONFormat {
  implicit val inputFormatConfigFormat = jsonFormat2(CollaborativeFilteringInputFormatConfig)
  implicit val outputFormatConfigFormat = jsonFormat2(CollaborativeFilteringOutputFormatConfig)
  implicit val configFormat = jsonFormat16(CollaborativeFilteringConfig)
}

import CollaborativeFilteringConfigJSONFormat._

/**
 * Wrapper so that we can use simpler API for getting configuration settings.
 *
 * All of the settings can go into one JSON string so we don't need a bunch of String
 * constants passed around.
 */
class CollaborativeFilteringConfiguration(other: Configuration) extends GiraphConfiguration(other) {

  private val ConfigPropertyName = "collaborativefiltering.config"

  def this() = {
    this(new Configuration)
  }

  /** make sure required properties are set */
  def validate(): Unit = {
    require(get(ConfigPropertyName) != null, "collaborativefiltering.config property was not set in the Configuration")
  }

  def getConfig: CollaborativeFilteringConfig = {
    JsonParser(get(ConfigPropertyName)).asJsObject.convertTo[CollaborativeFilteringConfig]
  }

  def setConfig(value: CollaborativeFilteringConfig): Unit = {
    set(ConfigPropertyName, value.toJson.compactPrint)
  }
}
