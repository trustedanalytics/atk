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

package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import java.sql.SQLException
import java.util.Properties
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.{ ExportHdfsJdbcArgs }
import org.trustedanalytics.atk.engine.frame.plugins.load.JdbcFunctions
import org.trustedanalytics.atk.engine.frame.{ SparkFrame }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }

// Implicits needed for JSON conversion 
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Export a frame to Jdbc table
 */
@PluginDoc(oneLine = "Write current frame to JDBC table.",
  extended = """Table will be created or appended to.
Export of Vectors is not currently supported.""")
class ExportHdfsJdbcPlugin extends SparkCommandPlugin[ExportHdfsJdbcArgs, UnitReturn] {

  /**
   * The name of the command
   */
  override def name: String = "frame/export_to_jdbc"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ExportHdfsJdbcArgs)(implicit invocation: Invocation) = 5

  /**
   * Export the frame to a jdbc table
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments jdbc arguments
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsJdbcArgs)(implicit invocation: Invocation): UnitReturn = {

    exportToHdfsJdbc(arguments)

  }

  /**
   * Exports to jdbc
   * @param arguments jdbc arguments
   */
  private def exportToHdfsJdbc(arguments: ExportHdfsJdbcArgs)(implicit invocation: Invocation): UnitReturn = {

    val frame: SparkFrame = arguments.frame
    val dataFrame = frame.rdd.toDataFrame

    // Set up the connection string
    val (dbConnectionString, username, password) = arguments.connectionString match {
      case Some(connect) => connect
      case None => JdbcFunctions.buildUrl(arguments.connectorType)
    }

    val connect = new Properties()
    connect.setProperty("username", username)
    connect.setProperty("password", password)

    dataFrame.write.jdbc(dbConnectionString, arguments.tableName, connect)
  }
}
