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

package org.trustedanalytics.atk.engine.plugin

import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphReference }
import org.trustedanalytics.atk.engine.frame.{ SparkFrameImpl, SparkFrame }
import org.trustedanalytics.atk.engine.graph._
import org.trustedanalytics.atk.engine.util.ConfigUtils

import scala.collection.JavaConversions._

import com.typesafe.config.{ ConfigFactory, ConfigList, ConfigValue }
import org.apache.spark.SparkContext
import org.apache.spark.engine.{ ProgressPrinter, SparkProgressListener }
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig, EngineImpl }

/**
 * Base trait for command plugins that need direct access to a SparkContext
 *
 * @tparam Argument the argument type for the command
 * @tparam Return the return type for the command
 */
trait SparkCommandPlugin[Argument <: Product, Return <: Product]
    extends CommandPlugin[Argument, Return] {

  override def engine(implicit invocation: Invocation): EngineImpl = invocation.asInstanceOf[SparkInvocation].engine

  /**
   * Name of the custom kryoclass this plugin needs.
   * kryoRegistrator = None means use JavaSerializer
   */
  def kryoRegistrator: Option[String] = Some("org.trustedanalytics.atk.engine.EngineKryoRegistrator")

  def sc(implicit invocation: Invocation): SparkContext = invocation.asInstanceOf[SparkInvocation].sparkContext

  // Frames
  implicit def frameRefToSparkFrame(frame: FrameReference)(implicit invocation: Invocation): SparkFrame = new SparkFrameImpl(frame, sc, engine.frames)
  implicit def frameEntityToSparkFrame(frameEntity: FrameEntity)(implicit invocation: Invocation): SparkFrame = frameRefToSparkFrame(frameEntity.toReference)

  // Vertex Frames
  implicit def frameRefToVertexSparkFrame(frame: FrameReference)(implicit invocation: Invocation): SparkVertexFrame = new SparkVertexFrameImpl(frame, sc, engine.frames, engine.graphs)
  implicit def frameEntityToVertexSparkFrame(frameEntity: FrameEntity)(implicit invocation: Invocation): SparkVertexFrame = frameRefToVertexSparkFrame(frameEntity.toReference)

  // Edge Frames
  implicit def frameRefToEdgeSparkFrame(frame: FrameReference)(implicit invocation: Invocation): SparkEdgeFrame = new SparkEdgeFrameImpl(frame, sc, engine.frames, engine.graphs)
  implicit def frameEntityToEdgeSparkFrame(frameEntity: FrameEntity)(implicit invocation: Invocation): SparkEdgeFrame = frameRefToEdgeSparkFrame(frameEntity.toReference)

  // Graphs
  implicit def graphRefToSparkGraph(graph: GraphReference)(implicit invocation: Invocation): SparkGraph = new SparkGraphImpl(graph, sc, engine.graphs)
  implicit def graphEntityToSparkGraph(graphEntity: GraphEntity)(implicit invocation: Invocation): SparkGraph = graphRefToSparkGraph(graphEntity.toReference)

  /**
   * Can be overridden by subclasses to provide a more specialized Invocation. Called before
   * calling the execute method.
   */
  override protected def customizeInvocation(invocation: Invocation, arguments: Argument): Invocation = {
    require(invocation.isInstanceOf[CommandInvocation], "Cannot invoke a CommandPlugin without a CommandInvocation")
    val commandInvocation = invocation.asInstanceOf[CommandInvocation]
    val sparkEngine: EngineImpl = commandInvocation.engine.asInstanceOf[EngineImpl]
    val sparkInvocation = new SparkInvocation(sparkEngine,
      invocation.user,
      commandInvocation.commandId,
      invocation.executionContext,
      commandInvocation.arguments,
      //TODO: Hide context factory behind a property on SparkEngine?
      null,
      commandInvocation.commandStorage,
      invocation.eventContext)
    sparkInvocation.copy(sparkContext = createSparkContextForCommand(arguments, sparkEngine.sparkContextFactory)(sparkInvocation))
  }

  def createSparkContextForCommand(arguments: Argument, sparkContextFactory: SparkContextFactory)(implicit invocation: SparkInvocation): SparkContext = {
    val cmd = invocation.commandStorage.expectCommand(invocation.commandId)
    val context: SparkContext = sparkContextFactory.context(s"(id:${cmd.id},name:${cmd.name})", kryoRegistrator)
    if (!EngineConfig.reuseSparkContext) {
      try {
        val listener = new SparkProgressListener(SparkProgressListener.progressUpdater, cmd, numberOfJobs(arguments)) // Pass number of Jobs here
        val progressPrinter = new ProgressPrinter(listener)
        context.addSparkListener(listener)
        context.addSparkListener(progressPrinter)
      }
      catch {
        // exception only shows up here due to dev error, but it is hard to debug without this logging
        case e: Exception => error("could not create progress listeners", exception = e)
      }
    }
    SparkCommandPlugin.commandIdContextMapping += (cmd.id -> context)
    context
  }

  override def cleanup(invocation: Invocation) = {
    val sparkInvocation = invocation.asInstanceOf[SparkInvocation]
    SparkCommandPlugin.stop(sparkInvocation.commandId)
  }

  /* plugins which execute python UDF will override this to true; by default this is false .
     if true, additional files are shipped for udf execution during a yarn job
   */
  def executesPythonUdf = false
}

object SparkCommandPlugin extends EventLogging {

  private var commandIdContextMapping = Map.empty[Long, SparkContext]

  def stop(commandId: Long) = {
    commandIdContextMapping.get(commandId).foreach { case (context) => stopContextIfNeeded(context) }
    commandIdContextMapping -= commandId
  }

  private def stopContextIfNeeded(sc: SparkContext): Unit = {
    if (EngineConfig.reuseSparkContext) {
      info("not stopping local SparkContext so that it can be re-used")
    }
    else {
      sc.stop()
    }
  }
}
