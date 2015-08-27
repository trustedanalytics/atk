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

package org.trustedanalytics.atk.rest.v1.decorators

import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.schema.Schema
import org.joda.time.DateTime
import org.scalatest.{ FlatSpec, Matchers }
import org.joda.time.DateTime
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Schema }

class FrameDecoratorTest extends FlatSpec with Matchers {

  val baseUri = "http://www.example.com/frames"
  val uri = baseUri + "/1"
  val frame = new FrameEntity(1, Some("name"), FrameSchema(), 1L, new DateTime, new DateTime)

  "FrameDecorator" should "be able to decorate a frame" in {
    val decoratedFrame = FrameDecorator.decorateEntity(uri, Nil, frame)
    decoratedFrame.name should be(Some("name"))
    decoratedFrame.uri should be("frames/1")
    decoratedFrame.entityType should be("frame:")
    decoratedFrame.links.head.uri should be("http://www.example.com/frames/1")
  }

  it should "set the correct URL in decorating a list of frames" in {
    val frameHeaders = FrameDecorator.decorateForIndex(baseUri, Seq(frame))
    val frameHeader = frameHeaders.toList.head
    frameHeader.url should be("http://www.example.com/frames/1")
  }

  it should "add a RelLink for error frames" in {
    val decoratedFrame = FrameDecorator.decorateEntity(uri, Nil, frame.copy(errorFrameId = Some(5)))

    decoratedFrame.links.size should be(2)

    // error frame link
    decoratedFrame.links.head.rel should be("ia-error-frame")
    decoratedFrame.links.head.uri should be("http://www.example.com/frames/5")

    // self link
    decoratedFrame.links.last.uri should be("http://www.example.com/frames/1")
  }
}
