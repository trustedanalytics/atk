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

package org.trustedanalytics.atk.engine.frame.plugins.load.JdbcPlugin

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc
import org.apache.spark._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.frame.{ FrameEntity }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.frame.plugins.load.LoadRddFunctions
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.frame.plugins.load.JdbcFunctions

object LoadFromJdbcArgsFormat {
  implicit val jdbcargsFormat = jsonFormat3(LoadFromJdbcArgs)
}

import LoadFromJdbcArgsFormat._

/**
 * Jdbc argument class
 * @param destination destination frame
 * @param tableName table name to read from
 * @param driverName optional driver name
 */
case class LoadFromJdbcArgs(
    @ArgDoc("""DataFrame to load data into.Should be either a uri or id.""") destination: FrameReference,
    @ArgDoc("""table name""") tableName: String,
    @ArgDoc("""(optional) connector type""") connectorType: String = "postgres") {

  require(StringUtils.isNotEmpty(tableName), "table name is required")
  require(connectorType == "postgres" || connectorType == "mysql", "connector type must be postgres or mysql")
}

/**
 * Parsing data to load and append to data frames
 */
@PluginDoc(oneLine = "Append data from a JDBC table into an existing (possibly empty) frame",
  extended = "Append data from a JDBC table into an existing (possibly empty) frame",
  returns = "the initial frame with the JDBC data appended")
class LoadFromJdbcPlugin extends SparkCommandPlugin[LoadFromJdbcArgs, FrameEntity] {

  /**
   * Command name
   */
  override def name: String = "frame/_loadjdbc"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(load: LoadFromJdbcArgs)(implicit invocation: Invocation) = 8

  /**
   * Parsing data to load and append to data frames
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LoadFromJdbcArgs)(implicit invocation: Invocation): FrameEntity = {
    val destinationFrame: SparkFrame = arguments.destination

    // run the operation
    val dataFrame = createDataFrame(sc, arguments)
    LoadRddFunctions.unionAndSave(destinationFrame, FrameRdd.toFrameRdd(dataFrame))
  }

  /**
   * Create a data frame from an jdbc compatible database
   * @param sc default spark context
   * @param arguments arguments for jdbc connection (including the initial data filtering)
   */
  def createDataFrame(sc: SparkContext, arguments: LoadFromJdbcArgs): DataFrame = {
    val sqlContext = new SQLContext(sc)
    val dbConnection = JdbcFunctions.buildUrl(arguments.connectorType)

    sqlContext.read.jdbc(dbConnection.urlString, arguments.tableName, dbConnection.userPassProperties)
  }

}
