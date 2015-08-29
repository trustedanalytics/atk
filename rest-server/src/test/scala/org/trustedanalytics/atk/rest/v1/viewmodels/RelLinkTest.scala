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

package org.trustedanalytics.atk.rest.v1.viewmodels

import org.scalatest.{ Matchers, FlatSpec }

class RelLinkTest extends FlatSpec with Matchers {

  "RelLink" should "be able to create a self link" in {

    val uri = "http://www.example.com/"
    val relLink = Rel.self(uri)

    relLink.rel should be("self")
    relLink.method should be("GET")
    relLink.uri should be(uri)
  }

  it should "not allow invalid methods" in {
    intercept[IllegalArgumentException] { RelLink("atk-foo", "uri", "WHACK") }
  }

  it should "not allow null" in {
    intercept[IllegalArgumentException] { RelLink(null, "uri", "GET") }
    intercept[IllegalArgumentException] { RelLink("rel", null, "GET") }
    intercept[IllegalArgumentException] { RelLink("rel", "uri", null) }
  }
}
