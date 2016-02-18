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

package org.trustedanalytics.atk.engine.command.mgmt

import java.net.URL

import org.trustedanalytics.atk.engine.{ EngineConfig, EngineExecutionContext }
import org.trustedanalytics.atk.engine.jobcontext.JobContextStorageImpl
import org.trustedanalytics.atk.event.EventLogging

/**
 * Shutdown hook asks all recently active yarn jobs to exit when REST server exists
 */
object YarnJobShutdownHook extends EventLogging {

  def createHook(jobContextStorageImpl: JobContextStorageImpl): Unit = {

    Runtime.getRuntime.addShutdownHook(new Thread("YarnJobShutdownHook") {

      override def run(): Unit = {
        info("shutdown hook running...")
        val recentlyActiveContexts = jobContextStorageImpl.lookupRecentlyActive(EngineConfig.yarnWaitTimeout.toInt)
        info(s"shutdown hook found ${recentlyActiveContexts.size} recently active jobContexts")
        recentlyActiveContexts.foreach(jobContext => {
          val uri = jobContext.jobServerUri
          if (uri.isDefined) {
            info(s"sending shutdown command to ${jobContext.getYarnAppName} at ${uri.get}")
            try {
              new YarnWebClient(new URL(uri.get)).shutdownServer()
              info(s"sending shutdown command to ${jobContext.getYarnAppName} was successful")
            }
            catch {
              case e: Exception => info(s"sending shutdown command to ${jobContext.getYarnAppName} wasn't successful, might already be shutdown: $e")
            }
          }
        })
      }
    })

  }

}
