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

import java.net.URL

import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.domain.jobcontext.JobContext
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.engine.command.mgmt.YarnWebClient
import org.trustedanalytics.atk.engine.plugin.{ Invocation, CommandPlugin }
import org.trustedanalytics.atk.engine.util.JvmMemory
import org.trustedanalytics.atk.moduleloader.ClassLoaderAware
import org.trustedanalytics.atk.{ EventLoggingImplicits, NotFoundException }
import spray.json._
import scala.concurrent._
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.util.Try
import org.trustedanalytics.atk.domain.command.{ CommandDefinition, CommandTemplate, Command }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import EngineExecutionContext.global

case class CommandContext(
    command: Command,
    executionContext: ExecutionContext,
    user: UserPrincipal,
    eventContext: EventContext) {

}

/**
 * CommandExecutor uses a registry of CommandPlugins and executes them on request.
 *
 * @param engine an Engine instance that will be passed to command plugins during execution
 * @param commands a command storage that the executor can use for audit logging command execution
 */
class CommandExecutor(engine: => EngineImpl, commands: CommandStorage, commandPluginRegistry: CommandPluginRegistry)
    extends EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  /**
   * Returns all the command definitions registered with this command executor.
   */
  def commandDefinitions: Iterable[CommandDefinition] = commandPluginRegistry.commandDefinitions

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * This overload requires that the command already is registered in the plugin registry using registerCommand.
   *
   * @param commandTemplate the CommandTemplate from which to extract the command name and the arguments
   * @return a Command record that can be used to track the command's execution
   */
  def executeInBackground(commandTemplate: CommandTemplate)(implicit invocation: Invocation): Command = {
    withMyClassLoader {
      withContext("ce.execute(ct)") {
        val cmd = commands.create(commandTemplate.copy(createdBy = if (invocation.user != null) Some(invocation.user.user.id) else None))
        validatePluginExists(cmd)
        future {
          executeInForeground(cmd)
        }
        cmd
      }
    }
  }

  def executeInForeground(cmd: Command)(implicit invocation: Invocation): Unit = {
    withContext(s"ce.executeInForeground.${cmd.name}") {
      validatePluginExists(cmd)
      val context = CommandContext(cmd, EngineExecutionContext.global, user, eventContext)
      executeCommandContext(context)
    }
  }

  /**
   * Execute commandContext in current thread (assumes correct classloader, context, etc. is already setup)
   * @tparam R plugin return type
   * @tparam A plugin arguments
   * @return plugin return value as JSON
   */
  private def executeCommandContext[R <: Product: TypeTag, A <: Product: TypeTag](commandContext: CommandContext)(implicit invocation: Invocation): Unit = withContext("cmdExcector") {

    info(s"command id:${commandContext.command.id}, name:${commandContext.command.name}, args:${commandContext.command.compactArgs}, ${JvmMemory.memory}")
    debug(s"System Properties are: ${sys.props.keys.mkString(",")}")

    val plugin = expectCommandPlugin[A, R](commandContext.command)
    plugin match {
      case commandPlugin: CommandPlugin[A, R] if !sys.props.contains("SPARK_SUBMIT") && EngineConfig.isSparkOnYarn =>

        val jobContext = engine.jobContextStorage.lookupOrCreate(invocation.user.user, commandContext.command.getJobName, invocation.clientId)
        engine.commandStorage.updateJobContextId(commandContext.command.id, jobContext.id)

        if (!notifyJob(jobContext)) {
          commands.complete(commandContext.command.id, Try {
            val moduleName = commandPluginRegistry.moduleNameForPlugin(plugin.name)
            new SparkSubmitLauncher(new FileStorage, engine).execute(commandContext.command, commandPlugin, moduleName, jobContext)

            // Reload the command as the error/result etc fields should have been updated in metastore upon yarn execution
            val updatedCommand = commands.expectCommand(commandContext.command.id)
            if (updatedCommand.error.isDefined) {
              error(s"Command id:${commandContext.command.id} plugin:${plugin.name} ${updatedCommand.error.get}")
              throw new Exception(s"Error executing ${plugin.name}: ${updatedCommand.error.get.message}")
            }
            if (updatedCommand.result.isEmpty) {
              error(s"Command didn't have any results, this is probably do to an error submitting command to yarn-cluster: $updatedCommand")
              throw new Exception(s"Error submitting command to yarn-cluster.")
            }
          })
        }
      case _ =>
        commands.complete(commandContext.command.id, Try {
          // here we are either in Yarn or we are running a command that doesn't need to run in Yarn
          val commandInvocation = new SimpleInvocation(engine, commands, commandContext, invocation.clientId)
          val arguments = plugin.parseArguments(commandContext.command.arguments.get)
          info(s"Invoking command ${commandContext.command.name}")
          val returnValue = plugin(commandInvocation, arguments)
          val jsonResult = plugin.serializeReturn(returnValue)
          engine.commandStorage.updateResult(commandContext.command.id, jsonResult)
        })
    }
  }

  /**
   * Notify the running job that there is a new command to execute
   * @param jobContext meta store record
   * @return true if successful, false means a new job needs to be launched
   */
  private def notifyJob(jobContext: JobContext): Boolean = {
    if (EngineConfig.keepYarnJobAlive && jobContext.jobServerUri.isDefined) {
      try {
        val uri = jobContext.jobServerUri.get
        new YarnWebClient(new URL(uri)).notifyServer()
        true
      }
      catch {
        case e: Exception =>
          info("couldn't send message: " + e)
          false
      }
    }
    else {
      // job was never created, so nothing to notify
      false
    }
  }

  /**
   * Throw exception if plugin doesn't exist for the supplied Command
   */
  private def validatePluginExists(command: Command): Unit = {
    expectCommandPlugin(command)
  }

  /**
   * Lookup the plugin from the registry
   * @param command the command to lookup a plugin for
   * @tparam A plugin arguments
   * @tparam R plugin return type
   * @return the plugin
   */
  private def expectCommandPlugin[A <: Product, R <: Product](command: Command): CommandPlugin[A, R] = {
    commandPluginRegistry.getCommandDefinition(command.name)
      .getOrElse(throw new NotFoundException("command definition", command.name))
      .asInstanceOf[CommandPlugin[A, R]]
  }

  /**
   * Cancel a command
   * @param commandId command id
   */
  def cancelCommand(commandId: Long): Unit = withMyClassLoader {
    // This should be killing any yarn jobs running
    val command = commands.lookup(commandId).getOrElse(throw new Exception(s"Command $commandId does not exist"))
    val commandPlugin = commandPluginRegistry.getCommandDefinition(command.name).get
    if (commandPlugin.isInstanceOf[SparkCommandPlugin[_, _]]) {
      if (!EngineConfig.isSparkOnYarn) {
        // SparkCommandPlugins which run on Standalone cluster mode
        SparkCommandPlugin.stop(commandId)
      }
      else {
        YarnUtils.killYarnJob(command.getJobName)
      }
    }
    else {
      error(s"Cancel is NOT implemented for anything other than SparkCommandPlugins, ${command.name} is NOT a SparkCommandPlugin")
    }
  }
}
