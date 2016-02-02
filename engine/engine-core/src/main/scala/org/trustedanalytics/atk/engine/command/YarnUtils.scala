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

package org.trustedanalytics.atk.engine.command

import org.trustedanalytics.atk.event.EventLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.api.records.YarnApplicationState

import scala.collection.JavaConversions._

object YarnUtils extends EventLogging {

  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    }
    finally {
      resource.close()
    }

  def initYarnClient(): YarnClient = {
    val yarnClient = YarnClient.createYarnClient
    val yarnConf = new YarnConfiguration(new Configuration())
    yarnClient.init(yarnConf)
    yarnClient
  }

  /* Kill Yarn Application given job name */
  def killYarnJob(jobName: String): Unit = {
    val yarnClient = initYarnClient()
    using[YarnClient, Unit](yarnClient) {
      yarnClient =>
        yarnClient.start()
        val app = yarnClient.getApplications.find(ap => ap.getName == jobName)
        if (app.isDefined) {
          info(s"Killing yarn application ${app.get.getApplicationId} which corresponds to command $jobName")
          yarnClient.killApplication(app.get.getApplicationId)
        }
    }
  }

  /* Get Yarn Application Id given job name */
  def getYarnJobId(jobName: String): String = {
    val yarnClient = initYarnClient()
    val allStates = java.util.EnumSet.copyOf(YarnApplicationState.values().toSeq)
    using[YarnClient, String](yarnClient) {
      yarnClient =>
        val app = yarnClient.getApplications(allStates).find(ap => ap.getName == jobName)
        app.get.getApplicationId.toString
    }
  }
}
