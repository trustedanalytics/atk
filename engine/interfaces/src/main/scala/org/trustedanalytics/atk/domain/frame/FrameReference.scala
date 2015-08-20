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

package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.domain.UriReference

case class FrameReference(frameId: Long) extends UriReference {
  /** The entity id */
  override def id: Long = frameId

  /** The entity root path, like "frames" or "graphs" */
  override def entityCollectionName: String = "frames"
}

object FrameReference {

  implicit def frameEntityToFrameReference(frameEntity: FrameEntity): FrameReference = frameEntity.toReference

  implicit def uriToFrameReference(uri: String): FrameReference = UriReference.fromString[FrameReference](uri, new FrameReference(_))

  implicit def idToFrameReference(id: Long): FrameReference = FrameReference(id)
}
