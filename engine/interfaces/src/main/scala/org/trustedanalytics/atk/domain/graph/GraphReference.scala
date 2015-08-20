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

package org.trustedanalytics.atk.domain.graph

import org.trustedanalytics.atk.domain.UriReference

case class GraphReference(graphId: Long) extends UriReference {

  override def id: Long = graphId

  override def entityCollectionName: String = "graphs"
}

object GraphReference {

  implicit def graphEntityToGraphReference(graphEntity: GraphEntity): GraphReference = graphEntity.toReference

  implicit def uriToGraphReference(uri: String): GraphReference = UriReference.fromString[GraphReference](uri, new GraphReference(_))

  implicit def idToGraphReference(id: Long): GraphReference = GraphReference(id)
}
