
package org.trustedanalytics.atk.engine.frame.plugins.exporthdfs

import java.sql.SQLException
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
   * Calculate covariance for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments jdbc arguments
   * @return value of type declared as the Return type
   */
  override def execute(arguments: ExportHdfsJdbcArgs)(implicit invocation: Invocation): UnitReturn = {

    val connectionArgs = JdbcFunctions.buildConnectionArgs(arguments.tableName, arguments.connectorType, arguments.url, arguments.driverName)
    exportToHdfsJdbc(arguments, connectionArgs)

  }

  /**
   * Exports to jdbc
   * @param arguments jdbc arguments
   */
  private def exportToHdfsJdbc(arguments: ExportHdfsJdbcArgs,
                               connectionArgs: Map[String, String])(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val dataFrame = frame.rdd.toDataFrame
    try {
      dataFrame.createJDBCTable(connectionArgs(JdbcFunctions.urlKey), connectionArgs(JdbcFunctions.dbTableKey), false)
    }
    catch {
      case e: SQLException =>
        dataFrame.insertIntoJDBC(connectionArgs(JdbcFunctions.urlKey), connectionArgs(JdbcFunctions.dbTableKey), false)
    }

  }

}
