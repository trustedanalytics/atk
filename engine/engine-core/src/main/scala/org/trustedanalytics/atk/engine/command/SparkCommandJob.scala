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

import java.net.InetAddress

import org.trustedanalytics.atk.engine.command.mgmt.{ YarnWebServer, JobManager }
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.domain.User
import org.trustedanalytics.atk.engine.plugin.{ Invocation, Call }
import org.trustedanalytics.atk.engine._
import com.typesafe.config.ConfigFactory
import org.apache.commons.lang3.exception.ExceptionUtils
import scala.reflect.io.Directory
import com.github.fommil.netlib

/**
 * The Yarn Job that runs a SparkCommandPlugin.
 *
 * First, SparkSubmitLauncher starts a SparkSubmit process.
 * Next, SparkSubmit starts a SparkCommandJob.
 * Finally, SparkCommandJob executes a SparkCommandPlugin.
 */
class SparkCommandJob(jobContextId: Long) extends AbstractEngineComponent {

  override lazy val commandLoader = new CommandLoader(loadFromModules = false)

  val jobContext = jobContextStorage.expectJobContext(jobContextId)

  val manager = new JobManager(EngineConfig.yarnWaitTimeout)

  var webserver: YarnWebServer = YarnWebServer.init(manager)
  // TODO: need better way to get the local host, this won't always work
  val uri = s"http://${InetAddress.getLocalHost.getHostAddress}:${webserver.getListeningPort}/"
  println(s"webserver URI: $uri")
  jobContextStorage.updateJobServerUri(jobContextId, uri)

  /**
   * Execute Command
   */
  def execute(): Unit = {

    while (manager.isKeepRunning) {
      if (manager.shouldDoWork()) {
        val commands = commandStorage.lookup(jobContext)
        info(s"executing ${commands.size} commands for jobContext $jobContext")
        for (command <- commands) {
          val user: Option[User] = command.createdById match {
            case Some(id) => metaStore.withSession("se.command.lookup") {
              implicit session =>
                metaStore.userRepo.lookup(id)
            }
            case _ => None
          }
          implicit val invocation: Invocation = new Call(user match {
            case Some(u) => userStorage.createUserPrincipalFromUser(u)
            case _ => null
          }, EngineExecutionContext.global, jobContext.clientId)
          commandExecutor.executeInForeground(command)(invocation)
        }
        manager.logActivity()
      }
      else {
        Thread.sleep(1000L)
      }
    }
    if (manager.shouldDoWork()) {
      // This shouldn't be able to happen unless you have a multi-thread client who calling release() while submitting work
      val msg = "job was asked to exit, so it is exiting, but it still had more work to do!"
      System.err.println(msg)
      throw new Exception(msg)
    }
  }

  /**
   * Shutdown the driver
   */
  def stop(): Unit = {
    println("shutting down the driver")
    if (webserver != null) {
      webserver.stop()
    }
  }
}

/**
 * The Yarn Job that runs a SparkCommandPlugin.
 *
 * First, SparkSubmitLauncher starts a SparkSubmit process.
 * Next, SparkSubmit starts a SparkCommandJob.
 * Finally, SparkCommandJob executes a SparkCommandPlugin.
 */
object SparkCommandJob {

  println(s"Java Class Path is: ${System.getProperty("java.class.path")}")
  println(s"Current PWD is ${Directory.Current.get.toString()}")

  /**
   * Usage string if this was being executed from the command line
   */
  def usage() = println("Usage: java -cp engine.jar org.trustedanalytics.atk.engine.commmand.SparkCommandJob <jobcontext_id>")

  /**
   * Entry point of SparkCommandJob for use by SparkSubmit.
   * @param args command line arguments.
   */
  def main(args: Array[String]) = {

    println(netlib.BLAS.getInstance().getClass().getName())
    println(netlib.LAPACK.getInstance().getClass().getName())

    if (args.length < 1) {
      usage()
    }
    else {
      if (EventLogging.raw) {
        val config = ConfigFactory.load()
        EventLogging.raw = if (config.hasPath("trustedanalytics.atk.engine.logging.raw")) config.getBoolean("trustedanalytics.atk.engine.logging.raw") else true
      } // else rest-server already installed an SLF4j adapter

      var driver: SparkCommandJob = null
      try {
        /* Set to true as for some reason in yarn cluster mode, this doesn't seem to be set on remote driver container */
        sys.props += Tuple2("SPARK_SUBMIT", "true")

        val jobContextId = args(0).toLong

        driver = new SparkCommandJob(jobContextId)
        driver.execute()
      }
      catch {
        case t: Throwable => error(s"Error captured in SparkCommandJob to prevent percolating up to ApplicationMaster + ${ExceptionUtils.getStackTrace(t)}")
      }
      finally {
        sys.props -= "SPARK_SUBMIT"
        if (driver != null) {
          driver.stop()
        }
      }
    }
  }
}
