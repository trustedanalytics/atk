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

import org.trustedanalytics.atk.domain.UserPrincipal
import org.trustedanalytics.atk.event.EventContext
import org.trustedanalytics.atk.engine.{ CommandProgressUpdater, EngineImpl }
import org.trustedanalytics.atk.engine.{ CommandStorageProgressUpdater, CommandStorage }
import spray.json.JsObject
import org.apache.spark.SparkContext

import scala.concurrent.ExecutionContext

/**
 * Captures details of a particular invocation of a command. This instance is passed to the
 * command's execute method along with the (converted) arguments supplied by the caller.
 *
 * @param engine an instance of the Engine for use by the command
 * @param user the calling user
 * @param commandId the ID assigned to this command execution
 * @param executionContext the Scala execution context in use
 * @param arguments the original JSON arguments, unconverted
 */
case class SparkInvocation(engine: EngineImpl,
                           user: UserPrincipal,
                           commandId: Long,
                           executionContext: ExecutionContext,
                           arguments: Option[JsObject],
                           sparkContext: SparkContext,
                           commandStorage: CommandStorage,
                           eventContext: EventContext,
                           clientId: String) extends CommandInvocation {
  override val progressUpdater: CommandProgressUpdater = new CommandStorageProgressUpdater(commandStorage)
}

object SparkInvocation {
  implicit def invocationToUser(inv: SparkInvocation): UserPrincipal = inv.user
  implicit def invocationToEventContext(inv: SparkInvocation): EventContext = inv.eventContext
}
