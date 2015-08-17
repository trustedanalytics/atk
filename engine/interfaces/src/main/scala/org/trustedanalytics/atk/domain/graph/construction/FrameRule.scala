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

package org.trustedanalytics.atk.domain.graph.construction

import org.trustedanalytics.atk.domain.frame.FrameReference

/**
 * Determines how to convert a dataframe into graph data.
 * @param frame Handle to the dataframe being read from.
 * @param vertexRules Rules for interpreting tabular data as vertices.
 * @param edgeRules Rules fo interpreting tabluar data as edges.
 */
case class FrameRule(frame: FrameReference, vertexRules: List[VertexRule], edgeRules: List[EdgeRule]) {
}
