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


package org.trustedanalytics.atk.engine.frame.plugins.dotproduct

import org.trustedanalytics.atk.domain.frame.FrameReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for dot-product plugin
 *
 * The dot-product plugin uses the left column values, and the right column values to compute the dot product for each row.
 * The column data may contain numbers, or lists of numbers.
 *
 */
case class DotProductArgs(frame: FrameReference,
                          @ArgDoc("""Names of columns used to create the left vector (A) for each row.
Names should refer to a single column of type vector, or two or more
columns of numeric scalars.""") leftColumnNames: List[String],
                          @ArgDoc("""Names of columns used to create right vector (B) for each row.
Names should refer to a single column of type vector, or two or more
columns of numeric scalars.""") rightColumnNames: List[String],
                          @ArgDoc("""Name of column used to store the
dot product.""") dotProductColumnName: String,
                          @ArgDoc("""Default values used to substitute null values in left vector.
Default is None.""") defaultLeftValues: Option[List[Double]] = None,
                          @ArgDoc("""Default values used to substitute null values in right vector.
Default is None.""") defaultRightValues: Option[List[Double]] = None) {
  require(frame != null, "frame is required")
  require(leftColumnNames.nonEmpty, "number of left columns cannot be zero")
  require(dotProductColumnName != null, "dot product column name cannot be null")
}
