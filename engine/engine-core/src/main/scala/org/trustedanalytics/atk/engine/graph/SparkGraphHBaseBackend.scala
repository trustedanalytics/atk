/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.graph

import java.util.UUID
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.{ EngineExecutionContext, GraphBackendStorage }
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.util.KerberosAuthenticator
import scala.concurrent.Future
import scala.util.{ Success, Failure }
import EngineExecutionContext.global

/**
 * Implements graph backend storage in HBase for Spark.
 */
class SparkGraphHBaseBackend(hbaseAdminFactory: HBaseAdminFactory)
    extends GraphBackendStorage
    with EventLogging
    with EventLoggingImplicits {

  /**
   * makes a copy of the graph's underlying table in the HBase
   *
   * @param graphName Name of the graph that is to copied
   * @param newName Name provided for the copy
   * @return
   */
  override def copyUnderlyingTable(graphName: String, newName: String)(implicit invocation: Invocation): Unit = {
    val tableName: String = graphName
    try {
      KerberosAuthenticator.loginWithKeyTabCLI()
      info(s"Trying to copy the HBase Table: $tableName")
      val hbaseAdmin = hbaseAdminFactory.createHBaseAdmin()

      if (hbaseAdmin.tableExists(tableName)) {
        info(s"Copying hbase table: $tableName to $newName")
        val snapshotName = tableName + "_copysnap_" + UUID.randomUUID().toString.replace("-", "")
        hbaseAdmin.snapshot(snapshotName, tableName)
        hbaseAdmin.cloneSnapshot(snapshotName, newName)
      }
      else {
        error(s"HBase table $tableName requested for copy does not exist.")
        throw new IllegalArgumentException(
          s"HBase table $tableName requested for copy does not exist.")
      }
    }
    catch {
      case ex: Exception =>
        info(s"Unable to copy the requested HBase table: $tableName.", exception = ex)
        throw new Exception(s"Unable to copy the requested HBase table $tableName.", ex)
    }
  }

  /**
   * Deletes a graph's underlying table from HBase.
   * @param graphName The user's name for the graph.
   * @param quiet Whether we attempt to delete quietly(if true) or raise raise an error if table doesn't exist(if false).
   * @param inBackground true to let the actual deletion happen sometime in the future, false to block until delete is complete
   *                           true will also prevent exceptions from bubbling up
   */
  override def deleteUnderlyingTable(graphName: String, quiet: Boolean, inBackground: Boolean)(implicit invocation: Invocation): Unit = withContext("deleteUnderlyingTable") {
    if (inBackground) {
      Future {
        performDelete(graphName, quiet)
      } onComplete {
        case Success(ok) => info(s"deleting table '$graphName' completed, quiet: $quiet, inBackground: $inBackground")
        case Failure(ex) => error(s"deleting table '$graphName' failed, quiet: $quiet, inBackground: $inBackground", exception = ex)
      }
      Unit
    }
    else {
      performDelete(graphName, quiet)
    }
  }

  private def performDelete(graphName: String, quiet: Boolean): Unit = {
    val tableName: String = graphName
    try {
      KerberosAuthenticator.loginWithKeyTabCLI()
      val hbaseAdmin = hbaseAdminFactory.createHBaseAdmin()

      if (hbaseAdmin.tableExists(tableName)) {
        if (hbaseAdmin.isTableEnabled(tableName)) {
          info(s"disabling hbase table: $tableName")
          hbaseAdmin.disableTable(tableName)
        }
        info(s"deleting hbase table: $tableName")
        hbaseAdmin.deleteTable(tableName)
      }
      else {
        info(s"HBase table $tableName requested for deletion does not exist.")
        if (!quiet) {
          throw new IllegalArgumentException(
            s"HBase table $tableName requested for deletion does not exist.")
        }
      }
    }
    catch {
      case e: Throwable =>
        error(s"Unable to delete the requested HBase table: $tableName.", exception = e)
        if (!quiet) {
          throw new IllegalArgumentException(
            s"Unable to delete the requested HBase table: $tableName.")
        }
    }
  }
}
