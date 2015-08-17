/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.engine

import scala.annotation.meta.field

// References:
// http://stackoverflow.com/questions/13944819/how-do-you-get-java-method-annotations-to-work-in-scala
// http://stackoverflow.com/questions/17792383/how-to-list-all-fields-with-a-custom-annotation-using-scalas-reflection-at-runt
// http://stackoverflow.com/questions/20710269/getting-scala-2-10-annotation-values-at-runtime
// http://www.veebsbraindump.com/2013/01/reflecting-annotations-in-scala-2-10/

package object plugin {
  type ArgDoc = ArgDocAnnotation @field
  type PluginDoc = PluginDocAnnotation
}

/**
 * Annotation to provide documentation for individual fields of plugin Arguments and Return
 * @param description text describing the arg (do not provide the name or type as they are provided with reflection)
 */
case class ArgDocAnnotation(description: String = "") extends scala.annotation.StaticAnnotation

/**
 * Annotation to provide documentation for a plugin
 * @param oneLine concise one-liner, ends with a period
 * @param extended rich description, can be several paragraphs
 * @param returns optional description for the return object.  Uses empty string for null value, instead of null
 *                for the sake of serialization, and instead of Option for the sake of reflection
 */
case class PluginDocAnnotation(oneLine: String, extended: String, returns: String = "") extends scala.annotation.StaticAnnotation {

  /**
   * Get description text of returns as Option, accounting for its empty semantics
   * @return
   */
  def getReturnsDescription: Option[String] = {
    if (returns.nonEmpty) { Some(returns) } else { None }
  }
}
