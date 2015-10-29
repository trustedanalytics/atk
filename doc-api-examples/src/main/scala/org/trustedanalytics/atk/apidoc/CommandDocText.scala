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

package org.trustedanalytics.atk.apidoc

import scala.io.Source

object CommandDocText {

  /**
   * Retrieves the text from the resource file according to command name
   * @param commandName full command name, like "frame/add_columns"
   * @param client client scope, like "python" (see resource folder structure)
   * @return file text (returns None if file not found)
   */
  def getText(commandName: String, client: String): Option[String] = {
    val path = "/" + client + "/" + commandName + ".rst"
    getClass.getResource(path) match {
      case null => None
      case r =>
        try { Some(Source.fromURL(r).mkString) }
        catch { case ex: Exception => throw new RuntimeException(s"CommandDocText problem at $path\n" + ex.toString) }
    }
  }
}
