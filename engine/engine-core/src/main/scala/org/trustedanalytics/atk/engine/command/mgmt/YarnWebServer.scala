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

package org.trustedanalytics.atk.engine.command.mgmt

import java.util.Date

import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.{ IHTTPSession, Response }

/**
 * Lightweight webserver for running in Yarn so that we can communicate
 * from the rest-server to a long-running Yarn job.
 */
class YarnWebServer(manager: JobManager) extends NanoHTTPD(YarnWebServer.Port) {

  override def serve(session: IHTTPSession): Response = {
    val message = session.getParms.get("message")
    try {
      manager.accept(message) // exception thrown if not accepted
      NanoHTTPD.newFixedLengthResponse("Message Accepted: " + new Date)
    }
    catch {
      case e: Exception => NanoHTTPD.newFixedLengthResponse(NanoHTTPD.Response.Status.INTERNAL_ERROR, "text/plain", e.getMessage)
    }
  }

}

object YarnWebServer {

  /** zero binds us to random port */
  val Port = 0
  val Timeout = 10000

  /**
   * Initialize a webserver for running in yarn
   * @return initialized webserver
   */
  def init(state: JobManager): YarnWebServer = {
    val webserver = new YarnWebServer(state)
    webserver.start(Timeout, false)
    webserver
  }

}
