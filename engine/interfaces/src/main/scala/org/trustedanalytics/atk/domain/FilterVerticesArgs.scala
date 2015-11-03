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


package org.trustedanalytics.atk.domain

import org.trustedanalytics.atk.domain.frame.Udf
import org.trustedanalytics.atk.domain.frame.FrameReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command to drop rows from a given vertex type.
 * @param udf filter expression
 */
case class FilterVerticesArgs(frame: FrameReference,
                              @ArgDoc("""UDF which evaluates a row to a boolean;
rows that evaluate to False are dropped from the Frame""") udf: Udf) {
  require(frame != null, "frame is required")
  require(udf != null, "udf is required")
}
