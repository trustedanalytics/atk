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


package org.trustedanalytics.atk.rest

import org.scalatest.{ Matchers, FlatSpec }

class UrlParserTest extends FlatSpec with Matchers {

  "UrlParser" should "be able to parse graphIds from graph URI's" in {
    val uri = "http://example.com/v1/graphs/34"
    UrlParser.getGraphId(uri) should be(Some(34))
  }

  it should "be able to parse frameIds from frame URI's" in {
    val uri = "http://example.com/v1/frames/55"
    UrlParser.getFrameId(uri) should be(Some(55))
  }

  it should "NOT parse frameIds from invalid URI's" in {
    val uri = "http://example.com/v1/invalid/55"
    UrlParser.getFrameId(uri) should be(None)
  }

  it should "NOT parse non-numeric frame ids" in {
    val uri = "http://example.com/v1/invalid/ABC"
    UrlParser.getFrameId(uri) should be(None)
  }
}
