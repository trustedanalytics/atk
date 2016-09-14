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

package org.trustedanalytics.atk.engine.frame.plugins.boxcox

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.FrameReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for box-cox plugin
 *
 * The box-cox plugin uses the values in columnName, and the lambda value to compute the box-cox transformation or it's reverse for each row.
 *
 */
case class BoxCoxArgs(frame: FrameReference,
                      @ArgDoc("""Names of column to perform transformation on""") columnName: String,
                      @ArgDoc("""Lambda""") lambdaValue: Double = 0d,
                      @ArgDoc("""Name of column used to store the transformation""") boxCoxColumnName: Option[String] = None) {
  require(frame != null, "frame is required")
  require(columnName.nonEmpty, "number of left columns cannot be zero")
}
/** Json conversion for arguments and return value case classes */
object BoxCoxJsonFormat {
  implicit val boxCoxFormat = jsonFormat4(BoxCoxArgs)
}
