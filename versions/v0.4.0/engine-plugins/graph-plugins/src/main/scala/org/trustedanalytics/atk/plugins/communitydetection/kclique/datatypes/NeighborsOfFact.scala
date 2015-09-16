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

package org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes

/**
 * Encodes the fact that all vertices of a given vertex set are neighbors of the vertex specified
 * by the NeighborsOf fact.
 *
 * A k neighbors-of fact is a neighbors-of fact in which the VertexSet contains exactly k vertices.
 * These are the kind of neighbors-of facts obtained when proceeding from the round k-1 to round k of the algorithm.
 *
 * INVARIANT:
 * when k is odd, every vertex ID in the VertexSet is less than the vertex ID in NeighborsOf.v
 * when k is even, every vertex ID in the VertexSet is greater than the vertex ID in NeighborsOf.v
 *
 */
case class NeighborsOfFact(members: VertexSet, neighbor: Long, neighborHigh: Boolean) extends Serializable
