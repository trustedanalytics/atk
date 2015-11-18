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
import org.trustedanalytics.atk.domain.User
import org.trustedanalytics.atk.engine.plugin.{ Invocation, Call }
import org.trustedanalytics.atk.engine._
import com.typesafe.config.ConfigFactory
import org.trustedanalytics.atk.moduleloader.Component
import org.apache.commons.lang3.exception.ExceptionUtils
import scala.reflect.io.Directory

/**
 * The Yarn Job that runs a SparkCommandPlugin.
 *
 * First, SparkSubmitLauncher starts a SparkSubmit process.
 * Next, SparkSubmit starts a SparkCommandJob.
 * Finally, SparkCommandJob executes a SparkCommandPlugin.
 */
class SparkCommandJob extends AbstractEngineComponent {

  override lazy val commandLoader = new CommandLoader(loadFromModules = false)

  /**
   * Execute Command
   * @param commandId id of command to execute
   */
  def execute(commandId: Long): Unit = {
    commands.lookup(commandId) match {
      case None => error(s"Command $commandId not found")
      case Some(command) =>
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
        }, EngineExecutionContext.global)
        commandExecutor.executeCommand(command)(invocation)
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
  def usage() = println("Usage: java -cp engine.jar org.trustedanalytics.atk.engine.commmand.SparkCommandJob <command_id>")

  /**
   * Instantiate an instance of the driver and then executing the requested command.
   * @param commandId the id of the Command to execute
   */
  def executeCommand(commandId: Long): Unit = {
    val driver = new SparkCommandJob
    driver.execute(commandId)
  }

  /**
   * Entry point of SparkCommandJob for use by SparkSubmit.
   * @param args command line arguments. Requires command id
   */
  def main(args: Array[String]) = {
    if (args.length < 1) {
      usage()
    }
    else {
      if (EventLogging.raw) {
        val config = ConfigFactory.load()
        EventLogging.raw = if (config.hasPath("trustedanalytics.atk.engine.logging.raw")) config.getBoolean("trustedanalytics.atk.engine.logging.raw") else true
      } // else rest-server already installed an SLF4j adapter

      try {
        /* Set to true as for some reason in yarn cluster mode, this doesn't seem to be set on remote driver container */
        sys.props += Tuple2("SPARK_SUBMIT", "true")
        val commandId = args(0).toLong
        executeCommand(commandId)
      }
      catch {
        case t: Throwable => error(s"Error captured in SparkCommandJob to prevent percolating up to ApplicationMaster + ${ExceptionUtils.getStackTrace(t)}")
      }
      finally {
        sys.props -= "SPARK_SUBMIT"
      }
    }
  }
}
