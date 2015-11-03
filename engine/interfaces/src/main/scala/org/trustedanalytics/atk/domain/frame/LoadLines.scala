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

import org.trustedanalytics.atk.domain.Partial
import org.trustedanalytics.atk.domain.schema.Schema

case class LoadLines[+Arguments](source: FileName,
                                 destination: FrameReference,
                                 skipRows: Option[Int],
                                 overwrite: Option[Boolean],
                                 lineParser: Partial[Arguments],
                                 schema: Schema) {
  require(source != null, "source is required")
  require(destination != null, "destination is required")
  require(skipRows.isEmpty || skipRows.get >= 0, "cannot skip negative number of rows")
  require(lineParser != null, "lineParser is required")
  require(schema != null, "schema is required")
}
