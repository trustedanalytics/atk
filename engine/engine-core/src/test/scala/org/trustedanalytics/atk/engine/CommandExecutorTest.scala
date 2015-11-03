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

import org.trustedanalytics.atk.domain.command.{ Command, CommandTemplate }
import org.trustedanalytics.atk.engine.command._
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation }
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ FlatSpec, Matchers }
import spray.json._

import scala.collection._
import scala.collection.immutable.HashMap
import scala.util.Try

class FakeCommandStorage extends CommandStorage {
  var commands: Map[Long, Command] = Map.empty
  var counter = 1L

  override def lookup(id: Long): Option[Command] = commands.get(id)

  override def scan(offset: Int, count: Int): Seq[Command] = commands.values.toSeq

  override def complete(id: Long, result: Try[JsObject]): Unit = {}
  override def storeResult(id: Long, result: Try[JsObject]): Unit = {}

  /**
   * update command info regarding progress of jobs initiated by this command
   * @param id command id
   * @param progressInfo list of progress for the jobs initiated by this command
   */
  override def updateProgress(id: Long, progressInfo: List[ProgressInfo]): Unit = {}

  override def create(template: CommandTemplate): Command = {
    val id = counter
    counter += 1
    val command: Command = Command(id, template.name, template.arguments, createdOn = DateTime.now, modifiedOn = DateTime.now)
    commands += (id -> command)
    command
  }

}

class CommandExecutorTest extends FlatSpec with Matchers with MockitoSugar {

  val loader = mock[CommandLoader]
  val commandRegistryMaps = CommandPluginRegistryMaps(new HashMap[String, CommandPlugin[_, _]], new HashMap[String, String])
  when(loader.loadFromConfig()).thenReturn(commandRegistryMaps)

  val commandPluginRegistry = new CommandPluginRegistry(loader)
  def createCommandExecutor(): CommandExecutor = {
    val engine = mock[EngineImpl]
    val commandStorage = new FakeCommandStorage
    val contextFactory = mock[SparkContextFactory]
    val sc = mock[SparkContext]
    when(contextFactory.context(anyString(), Some(anyString()))(any[Invocation])).thenReturn(sc)

    new CommandExecutor(engine, commandStorage, commandPluginRegistry)
  }

}
