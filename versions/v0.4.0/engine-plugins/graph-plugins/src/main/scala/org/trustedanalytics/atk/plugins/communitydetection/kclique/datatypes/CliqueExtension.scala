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
 * Encodes the fact that a given VertexSet forms a clique, and that the clique can be extended by adding
 * any one of the vertices from the set neighbors.
 *
 * A k clique-extension fact is a clique extension fact where the vertex set contains exactly k vertices.
 * These are the extension facts obtained after the k'th round of the algorithm.
 *
 * Symbolically, a pair (C,V) where:
 *   - C is a (k-1) clique.
 *   - For every v in V,  C + v is a k clique.
 *   - For all v so that C + v is a k clique, v is in V.
 *
 * INVARIANT:
 * when k is odd, every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
 * when k is even, every vertex ID in the VertexSet is greater than every vertex ID in the ExtenderSet.
 *
 */
case class CliqueExtension(clique: Clique, neighbors: VertexSet, neighborsHigh: Boolean) extends Serializable
