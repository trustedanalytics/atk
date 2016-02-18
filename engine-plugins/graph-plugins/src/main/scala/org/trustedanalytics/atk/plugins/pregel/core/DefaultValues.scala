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
package org.trustedanalytics.atk.plugins.pregel.core

/**
 * Companion object holds the default values.
 */
object DefaultValues {
  val edgeWeightDefault = 1.0d
  val powerDefault = 0d
  val smoothingDefault = 1.0d
  val priorDefault = 1d
  val deltaDefault = 0d
  val separatorDefault: Array[Char] = Array(' ', ',', '\t')
}

object Initializers extends Serializable {

  /**
   * Default message set.
   * @return an empty map
   */
  def defaultMsgSet(): Map[Long, Vector[Double]] = {
    Map().asInstanceOf[Map[Long, Vector[Double]]]
  }

  /**
   * Default edge set.
   * @return an empty map
   */
  def defaultEdgeSet(): Set[(Long, Long)] = {
    Set().asInstanceOf[Set[(Long, Long)]]
  }

  /**
   * Default vertex set.
   * @return an empty map
   */
  def defaultVertexSet(): Set[Long] = {
    Set().asInstanceOf[Set[Long]]
  }
}

