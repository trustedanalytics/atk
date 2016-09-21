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
package org.apache.spark.h2o.backends.internal

import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.listeners.ExecutorAddNotSupportedListener
import water.api.RestAPIManager
import water.{ H2O, H2OStarter }

class AtkInternalH2OBackend(hc: H2OContext) extends InternalH2OBackend(hc) {

  /** Initialize Sparkling H2O and start H2O cloud. */
  override def init(): Array[NodeDesc] = {

    logInfo(s"Starting H2O services: " + hc.getConf.toString)
    // Create dummy RDD distributed over executors
    val (spreadRDD, spreadRDDNodes) = new SpreadRDDBuilder(hc, InternalBackendUtils.guessTotalExecutorSize(hc.sparkContext)).build()

    if (hc.getConf.isClusterTopologyListenerEnabled) {
      // Attach listener which kills H2O cluster when new Spark executor has been launched ( which means
      // that this executors hasn't been discovered during the spreadRDD phase)
      hc.sparkContext.addSparkListener(new ExecutorAddNotSupportedListener())
    }

    // Start H2O nodes
    // Get executors to execute H2O
    val allExecutorIds = spreadRDDNodes.map(_.nodeId).distinct
    val executorIds = allExecutorIds
    // The collected executors based on IDs should match
    assert(spreadRDDNodes.length == executorIds.length,
      s"Unexpected number of executors ${spreadRDDNodes.length}!=${executorIds.length}")
    // H2O is executed only on the subset of Spark cluster - fail
    if (executorIds.length < allExecutorIds.length) {
      throw new IllegalArgumentException(s"""Spark cluster contains ${allExecutorIds.length},
               but H2O is running only on ${executorIds.length} nodes!""")
    }
    // Execute H2O on given nodes
    logInfo(s"""Launching H2O on following ${spreadRDDNodes.length} nodes: ${spreadRDDNodes.mkString(",")}""")

    var h2oNodeArgs = InternalBackendUtils.getH2ONodeArgs(hc.getConf)
    // Disable web on h2o nodes in non-local mode
    if (!hc.sparkContext.isLocal) {
      h2oNodeArgs = h2oNodeArgs ++ Array("-disable_web")
    }
    logDebug(s"Arguments used for launching h2o nodes: ${h2oNodeArgs.mkString(" ")}")
    val executors = AtkInternalBackendUtils.startH2O(hc.sparkContext, spreadRDD, spreadRDDNodes.length, h2oNodeArgs, hc.getConf.nodeNetworkMask)

    // Connect to a cluster via H2O client, but only in non-local case
    if (!hc.sparkContext.isLocal) {
      logTrace("Sparkling H2O - DISTRIBUTED mode: Waiting for " + executors.length)
      // Get arguments for this launch including flatfile ( Do not use IP if network mask is specified)
      val h2oClientArgs = InternalBackendUtils.toH2OArgs(InternalBackendUtils.getH2OClientArgs(hc.getConf), hc.getConf, executors)
      logDebug(s"Arguments used for launching h2o client node: ${h2oClientArgs.mkString(" ")}")
      // Launch H2O
      H2OStarter.start(h2oClientArgs, false)
    }
    // And wait for right cluster size
    H2O.waitForCloudSize(executors.length, hc.getConf.cloudTimeout)

    // Register web API for client
    RestAPIManager(hc).registerAll()
    H2O.finalizeRegistration()
    executors
  }
}
