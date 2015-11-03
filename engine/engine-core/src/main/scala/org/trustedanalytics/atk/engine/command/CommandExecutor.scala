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

import org.trustedanalytics.atk.component.ClassLoaderAware
import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, CommandPlugin }
import org.trustedanalytics.atk.engine.util.JvmMemory
import org.trustedanalytics.atk.{ EventLoggingImplicits, NotFoundException }
import spray.json._
import scala.concurrent._
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.util.Try
import org.trustedanalytics.atk.domain.command.{ CommandDefinition, CommandTemplate, Execution, Command }
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
 * CommandExecutor manages a registry of CommandPlugins and executes them on request.
 *
 * The plugin registry is based on configuration - all Archives listed in the configuration
 * file under trustedanalytics.atk.engine.archives will be queried for the "command" key, and
 * any plugins they provide will be added to the plugin registry.
 *
 * Plugins can also be added programmatically using the registerCommand method.
 *
 * Plugins can be executed in three ways:
 *
 * 1. A CommandPlugin can be passed directly to the execute method. The command need not be in
 * the registry
 * 2. A command can be called by name. This requires that the command be in the registry.
 * 3. A command can be called with a CommandTemplate. This requires that the command named by
 * the command template be in the registry, and that the arguments provided in the CommandTemplate
 * can be parsed by the command.
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
   * @return an Execution object that can be used to track the command's execution
   */
  def execute[A <: Product, R <: Product](commandTemplate: CommandTemplate)(implicit invocation: Invocation): Execution =
    withContext("ce.execute(ct)") {
      val cmd = commands.create(commandTemplate.copy(createdBy = if (invocation.user != null) Some(invocation.user.user.id) else None))
      validatePluginExists(cmd)
      val context = CommandContext(cmd, EngineExecutionContext.global, user, eventContext)
      Execution(cmd, executeCommandContextInFuture(context))
    }

  def executeCommand[A <: Product, R <: Product](cmd: Command)(implicit invocation: Invocation): Unit =
    withContext(s"ce.executeCommand.${cmd.name}") {
      validatePluginExists(cmd)
      val context = CommandContext(cmd, EngineExecutionContext.global, user, eventContext)

      // Stores the (intermediate) results, don't mark the command complete yet as it will be marked complete by rest server
      commands.storeResult(context.command.id, Try { executeCommandContext(context) })
    }

  /**
   * Execute the command in the future with correct classloader, context, etc.,
   * On complete - mark progress as 100% or failed
   */
  private def executeCommandContextInFuture[T](commandContext: CommandContext)(implicit invocation: Invocation): Future[Command] = {
    withMyClassLoader {
      withContext(commandContext.command.name) {
        // run the command in a future so that we don't make the client wait for initial response
        val cmdFuture = future {
          commands.complete(commandContext.command.id, Try {
            executeCommandContext(commandContext)
          })
          // get the latest command progress from DB when command is done executing
          commands.expectCommand(commandContext.command.id)
        }
        cmdFuture
      }
    }
  }

  /**
   * Execute commandContext in current thread (assumes correct classloader, context, etc. is already setup)
   * @tparam R plugin return type
   * @tparam A plugin arguments
   * @return plugin return value as JSON
   */
  private def executeCommandContext[R <: Product: TypeTag, A <: Product: TypeTag](commandContext: CommandContext)(implicit invocation: Invocation): JsObject = withContext("cmdExcector") {

    info(s"command id:${commandContext.command.id}, name:${commandContext.command.name}, args:${commandContext.command.compactArgs}, ${JvmMemory.memory}")
    debug(s"System Properties are: ${sys.props.keys.mkString(",")}")

    val plugin = expectCommandPlugin[A, R](commandContext.command)
    plugin match {
      case sparkCommandPlugin: SparkCommandPlugin[A, R] if !sys.props.contains("SPARK_SUBMIT") && EngineConfig.isSparkOnYarn =>
        val archiveName = commandPluginRegistry.getArchiveNameFromPlugin(plugin.name)
        new SparkSubmitLauncher().execute(commandContext.command, sparkCommandPlugin, archiveName)
        // Reload the command as the error/result etc fields should have been updated in metastore upon yarn execution
        val updatedCommand = commands.expectCommand(commandContext.command.id)
        if (updatedCommand.error.isDefined) {
          throw new scala.Exception(s"Error executing ${plugin.name}: ${updatedCommand.error.get.message}")
        }
        if (updatedCommand.result.isDefined) {
          updatedCommand.result.get
        }
        else {
          error(s"Command didn't have any results, this is probably do to an error submitting command to yarn-cluster: $updatedCommand")
          throw new scala.Exception(s"Error submitting command to yarn-cluster.")
        }
      case _ =>
        val commandInvocation = new SimpleInvocation(engine, commands, commandContext)
        val arguments = plugin.parseArguments(commandContext.command.arguments.get)
        info(s"Invoking command ${commandContext.command.name}")
        val returnValue = plugin(commandInvocation, arguments)
        plugin.serializeReturn(returnValue)
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
      // TODO: Other plugins like Giraph etc which inherit from CommandPlugin, See TRIB-4661
      // TODO: We need to rename all giraph jobs to use command.getJobName as yarn job name instead of hardcoded values like "iat_giraph_als"
      error(s"Cancel is NOT implemented for anything other than SparkCommandPlugins, ${command.name} is NOT a SparkCommandPlugin")
    }
  }
}
