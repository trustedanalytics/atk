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

import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.domain.command.{ Command, CommandDefinition, CommandTemplate, Execution }
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphTemplate }
import org.trustedanalytics.atk.domain.model.ModelEntity
import org.trustedanalytics.atk.domain.frame.RowQueryArgs
import org.trustedanalytics.atk.engine.plugin.Invocation

import scala.concurrent.Future

trait Engine {

  type Identifier = Long //TODO: make more generic?

  val frames: FrameStorage

  val graphs: GraphStorage

  val models: ModelStorage

  /**
   * Executes the given command template, managing all necessary auditing, contexts, class loaders, etc.
   *
   * Stores the results of the command execution back in the persistent command object.
   *
   * @param command the command to run, including name and arguments
   * @return an Execution that can be used to track the completion of the command
   */
  def execute(command: CommandTemplate)(implicit invocation: Invocation): Execution

  /**
   * All the command definitions available
   */
  def getCommandDefinitions()(implicit invocation: Invocation): Iterable[CommandDefinition]

  def getCommands(offset: Int, count: Int)(implicit invocation: Invocation): Future[Seq[Command]]

  def getCommand(id: Identifier)(implicit invocation: Invocation): Future[Option[Command]]

  def getUserPrincipal(userKey: String)(implicit invocation: Invocation): UserPrincipal

  def addUserPrincipal(userKey: String)(implicit invocation: Invocation): UserPrincipal

  def getFrame(id: Identifier)(implicit invocation: Invocation): Future[Option[FrameEntity]]

  def getRows(arguments: RowQueryArgs[Identifier])(implicit invocation: Invocation): QueryResult

  @deprecated("use engine.graphs.createFrame()")
  def createFrame(arguments: CreateEntityArgs)(implicit invocation: Invocation): Future[FrameEntity]

  def dropFrame(id: Identifier)(implicit invocation: Invocation): Future[Unit]

  def getFrames()(implicit invocation: Invocation): Future[Seq[FrameEntity]]

  def getFrameByName(name: String)(implicit invocation: Invocation): Future[Option[FrameEntity]]

  def shutdown(): Unit

  def getGraph(id: Identifier)(implicit invocation: Invocation): Future[GraphEntity]

  def getGraphs()(implicit invocation: Invocation): Future[Seq[GraphEntity]]

  def getGraphByName(name: String)(implicit invocation: Invocation): Future[Option[GraphEntity]]

  @deprecated("use engine.graphs.createGraph()")
  def createGraph(graph: GraphTemplate)(implicit invocation: Invocation): Future[GraphEntity]

  def getVertex(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[FrameEntity]

  def getVertices(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]]

  def getEdge(graphId: Identifier, label: String)(implicit invocation: Invocation): Future[FrameEntity]

  def getEdges(graphId: Identifier)(implicit invocation: Invocation): Future[Seq[FrameEntity]]

  def dropGraph(graphId: Identifier)(implicit invocation: Invocation): Future[Unit]

  def createModel(arguments: CreateEntityArgs)(implicit invocation: Invocation): Future[ModelEntity]

  def getModel(id: Identifier)(implicit invocation: Invocation): Future[ModelEntity]

  def getModels()(implicit invocation: Invocation): Future[Seq[ModelEntity]]

  def getModelByName(name: String)(implicit invocation: Invocation): Future[Option[ModelEntity]]

  def dropModel(id: Identifier)(implicit invocation: Invocation): Future[Unit]

  /**
   * Cancel a running command
   * @param id command id
   * @return optional command instance
   */
  def cancelCommand(id: Identifier)(implicit invocation: Invocation): Future[Unit]

}
