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
package org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes

/**
 * Represents a vertex in adjacency list form
 *
 * @param id The unique identifier of this vertex.
 * @param neighbors The list of this vertex's neighbors.
 */
case class VertexInAdjacencyFormat(id: Long, neighbors: Array[Long]) extends Serializable
