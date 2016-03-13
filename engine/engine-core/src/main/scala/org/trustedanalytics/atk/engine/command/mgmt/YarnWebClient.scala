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
package org.trustedanalytics.atk.engine.command.mgmt

import java.net.{ HttpURLConnection, URL }

/**
 * Client for YarnWebServer
 * @param baseUrl the base url for the YarnWebServer
 */
class YarnWebClient(baseUrl: URL) {

  def notifyServer(): Unit = {
    sendMsg(YarnWebProtocol.NextMsg)
  }

  def shutdownServer(): Unit = {
    sendMsg(YarnWebProtocol.ShutdownMsg)
  }

  private def sendMsg(msg: String): Unit = {
    val url = new URL(baseUrl.getProtocol, baseUrl.getHost, baseUrl.getPort, "?message=" + msg)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.connect()
      if (connection.getResponseCode != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException(s"Failed to send message '$msg', response was: ${connection.getResponseCode} ${connection.getResponseMessage}")
      }
    }
    finally {
      connection.disconnect()
    }
  }
}
