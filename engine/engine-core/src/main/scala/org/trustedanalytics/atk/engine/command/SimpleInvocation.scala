/**
 *  Copyright (c) 2016 Intel Corporation 
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

import org.trustedanalytics.atk.domain.UserPrincipal
import org.trustedanalytics.atk.event.EventContext
import org.trustedanalytics.atk.engine.plugin.CommandInvocation
import org.trustedanalytics.atk.engine.Engine
import spray.json.JsObject

import scala.concurrent.ExecutionContext

/**
 * Basic invocation for commands that don't need Spark
 */
case class SimpleInvocation(engine: Engine,
                            executionContext: ExecutionContext,
                            arguments: Option[JsObject],
                            commandId: Long,
                            user: UserPrincipal,
                            eventContext: EventContext,
                            clientId: String) extends CommandInvocation {

  def this(engine: Engine, commandContext: CommandContext, clientId: String) = {
    this(engine,
      commandContext.executionContext,
      commandContext.command.arguments,
      commandContext.command.id,
      commandContext.user,
      commandContext.eventContext,
      clientId)
  }

}
