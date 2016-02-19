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

package org.trustedanalytics.atk.engine.jobcontext

import java.net.URL

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.NoArgs
import org.trustedanalytics.atk.engine.EngineImpl
import org.trustedanalytics.atk.engine.command.mgmt.YarnWebClient
import org.trustedanalytics.atk.engine.plugin.{ Invocation, CommandPlugin }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Plugin for shutting down a Yarn application
 */
class JobContextReleasePlugin extends CommandPlugin[NoArgs, UnitReturn] {

  override def name: String = "_admin:/_release"

  override def execute(arguments: NoArgs)(implicit context: Invocation): UnitReturn = {
    val jobContextStorage = engine.asInstanceOf[EngineImpl].jobContextStorage
    jobContextStorage.lookupByClientId(context.user.user, context.clientId) match {
      case Some(jobContext) =>
        jobContext.jobServerUri match {
          case Some(uri) =>
            info(s"releasing client ${jobContext.clientId} at $uri")
            try {
              new YarnWebClient(new URL(uri)).shutdownServer()
            }
            catch {
              case e: Exception => info(s"error releasing client: $e")
            }
          case None => info(s"no job server uri so release() has nothing to do")
        }
      case None => info(s"nothing to do, clientId ${context.clientId} for user ${context.user.user} not found ")
    }
  }
}
