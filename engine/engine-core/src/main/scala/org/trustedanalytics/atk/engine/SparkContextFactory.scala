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

package org.trustedanalytics.atk.engine

import org.apache.spark.engine.{ JobContextProgressListener, ProgressPrinter }
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.engine.plugin.{ SparkInvocation, Invocation }
import org.trustedanalytics.atk.engine.util.KerberosAuthenticator
import org.trustedanalytics.atk.event.EventLogging
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{ SparkConf, SparkContext }

import scala.reflect.io.Directory

/**
 * Class Factory for creating spark contexts
 */
trait SparkContextFactory extends EventLogging with EventLoggingImplicits {

  /**
   * Creates a new sparkContext
   */
  def context(commandId: Long, commandName: String, kryoRegistrator: Option[String] = None)(implicit invocation: SparkInvocation): SparkContext = withContext("engine.SparkContextFactory") {
    val description = s"(id:$commandId,name:$commandName)"
    val sparkContext = if (EngineConfig.reuseSparkContext || EngineConfig.isSparkOnYarn) {
      SparkContextFactory.sharedSparkContext()
    }
    else {
      createContext(description, kryoRegistrator)
    }
    info(s"Setting Job Group Id as $commandId")
    sparkContext.setJobGroup(s"$commandId", description)
    sparkContext
  }

  private def createContext(description: String, kryoRegistrator: Option[String] = None)(implicit invocation: SparkInvocation): SparkContext = {
    val userName = user.user.apiKey.getOrElse(
      throw new RuntimeException("User didn't have an apiKey which shouldn't be possible if they were authenticated"))
    val sparkConf = new SparkConf()
      .setMaster(EngineConfig.sparkMaster)
      .setSparkHome(EngineConfig.sparkHome)
      .setAppName(s"trustedanalytics:$userName:$description")

    EngineConfig.sparkConfProperties.foreach { case (k, v) => debug(s"$k->$v") }
    sparkConf.setAll(EngineConfig.sparkConfProperties)

    if (!EngineConfig.disableKryo && kryoRegistrator.isDefined) {
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      sparkConf.set("spark.kryo.registrator", kryoRegistrator.get)
    }

    KerberosAuthenticator.loginUsingHadoopUtils()
    KerberosAuthenticator.loginAsAuthenticatedUser()

    info("SparkConf settings: " + sparkConf.toDebugString)

    val context = new SparkContext(sparkConf)
      .setCheckpointDir(EngineConfig.checkPointDirectory)
    if (!EngineConfig.reuseSparkContext) {
      try {
        val progressPrinter = new ProgressPrinter
        context.addSparkListener(progressPrinter)
        context.addSparkListener(new JobContextProgressListener(invocation.engine.jobContextStorage, invocation))
      }
      catch {
        // exception only shows up here due to dev error, but it is hard to debug without this logging
        case e: Exception => error("could not create progress listeners", exception = e)
      }
    }
    context
  }

}

object SparkContextFactory extends SparkContextFactory {

  private var sc: SparkContext = null

  private def sharedSparkContext()(implicit invocation: SparkInvocation): SparkContext = {
    this.synchronized {
      if (sc == null) {
        sc = createContext("reused-spark-context", None)
      }
    }
    sc
  }

}
