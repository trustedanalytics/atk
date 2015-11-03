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


package org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes

/**
 * Represents an undirected edge as a pair of vertex identifiers.
 *
 * To avoid duplicate entries of (v,u) and (u,v) for the edge {u,v} we require that the source be less than the
 * destination.
 *
 * @param source Source of the edge.
 * @param destination Destination of the edge.
 */
case class Edge(source: Long, destination: Long) extends Serializable {
  require(source < destination)
}

/**
 * Companion object for Edge class that provides the constructor.
 */
object Edge {
  def edgeFactory(u: Long, v: Long) = {
    require(u != v)
    new Edge(math.min(u, v), math.max(u, v))
  }
}
