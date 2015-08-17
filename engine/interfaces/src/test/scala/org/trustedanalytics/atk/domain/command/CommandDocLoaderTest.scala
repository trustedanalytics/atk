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

package org.trustedanalytics.atk.domain.command
import org.scalatest.{ FlatSpec, Matchers }
import org.scalatest.PrivateMethodTester._

class CommandDocLoaderTest extends FlatSpec with Matchers {

  "createCommandDoc" should "split text into two pieces" in {
    val createCommandDoc = PrivateMethod[Option[CommandDoc]]('createCommandDoc)
    val doc = CommandDocLoader invokePrivate createCommandDoc(
      Some("""    One-liner.

    Line 3
    Line 4"""))
    doc should not be None
    doc.get.oneLineSummary should be("One-liner.")
    doc.get.extendedSummary.get should be("""

    Line 3
    Line 4""")
    println(doc)
  }

  "getPath" should "replace special chars" in {
    val getPath = PrivateMethod[String]('getPath)
    CommandDocLoader invokePrivate getPath("frame:vertex/count") should be("frame-vertex/count")
  }
}
