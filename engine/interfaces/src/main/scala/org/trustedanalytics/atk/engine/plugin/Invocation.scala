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
import org.trustedanalytics.atk.engine.{ CommandStorage, Engine }
import spray.json.JsObject

import scala.concurrent.ExecutionContext
import org.trustedanalytics.atk.engine.CommandProgressUpdater

/**
 * Provides context for an invocation of a command or query.
 *
 */
trait Invocation {
  /**
   * The user that invoked the operation
   */
  private[trustedanalytics] def user: UserPrincipal

  /**
   * A Scala execution context for use with methods that require one
   */
  private[trustedanalytics] def executionContext: ExecutionContext

  /**
   * EventContext of the caller
   */
  private[trustedanalytics] def eventContext: EventContext

  /**
   * Update the progress
   * @param progress current progress
   */
  private[trustedanalytics] def updateProgress(progress: Float): Unit = ???
}

/**
 * Invocation class to be used by Engine when performing operations by scheduled tasks
 * @param executionContext A Scala execution context for use with methods that require one
 */
case class BackendInvocation(executionContext: ExecutionContext) extends Invocation {
  private[trustedanalytics] def user: UserPrincipal = null
  private[trustedanalytics] def eventContext: EventContext = null
}

case class Call(user: UserPrincipal,
                executionContext: ExecutionContext,
                eventContext: EventContext = null) extends Invocation

trait CommandInvocation extends Invocation {
  /**
   * An instance of the engine that the plugin can use to execute its work
   */
  private[trustedanalytics] def engine: Engine

  /**
   * The identifier of this execution
   */
  private[trustedanalytics] def commandId: Long

  /**
   * The original arguments as supplied by the user
   */
  private[trustedanalytics] def arguments: Option[JsObject]

  /**
   * Command Storage to read/update command progress
   */
  private[trustedanalytics] def commandStorage: CommandStorage

  val progressUpdater: CommandProgressUpdater

  override private[trustedanalytics] def updateProgress(progress: Float): Unit = progressUpdater.updateProgress(commandId, progress)
}

object Invocation {
  implicit def invocationToUser(implicit inv: Invocation): UserPrincipal = inv.user
  implicit def invocationToEventContext(implicit inv: Invocation): EventContext = inv.eventContext
}
