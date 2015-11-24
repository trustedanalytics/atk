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

package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

case class CopyFrameArgs(frame: FrameReference,
                         @ArgDoc("""dictionary of column names to include in the copy and target names""") columns: Option[Map[String, String]] = None,
                         @ArgDoc("""UDF for selecting what rows to copy""") where: Option[Udf] = None,
                         @ArgDoc("""name of the frame copy""") name: Option[String] = None) {
  require(frame != null, "frame is required")
  require(name != null, "name cannot be null")
  if (name.isDefined) FrameName.validate(name.get)
}
