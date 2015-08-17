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

package org.trustedanalytics.atk.domain

import org.scalatest.FlatSpec
import org.joda.time.DateTime

class UserTest extends FlatSpec {

  "User" should "be able to have a none apiKey" in {
    new User(1L, None, None, new DateTime, new DateTime)
  }

  it should "not be able to have a null apiKey" in {
    intercept[IllegalArgumentException] { new User(1L, None, null, new DateTime, new DateTime) }
  }

  it should "not be able to have an empty string apiKey" in {
    intercept[IllegalArgumentException] { new User(1L, None, Some(""), new DateTime, new DateTime) }
  }

  it should "have id greater than zero" in {
    intercept[IllegalArgumentException] { new User(-1L, None, Some("api"), new DateTime, new DateTime) }
  }
}
