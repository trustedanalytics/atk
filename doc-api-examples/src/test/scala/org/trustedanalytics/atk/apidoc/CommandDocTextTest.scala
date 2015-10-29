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


package org.trustedanalytics.atk.apidoc

import org.scalatest.{ FlatSpec, Matchers }

class CommandDocTextTest extends FlatSpec with Matchers {

  "getDocText" should "return None if resource file not found" in {
    CommandDocText.getText("entity/bogus", "test") should be(None)
  }

  "getDocText" should "find resource file and return contents" in {
    val doc = CommandDocText.getText("entity/function", "test")
    doc should not be None
    doc.get should be("""    One line summary with period.

    Extended Summary
    ----------------
    Extended summary about the function which may be
    multiple lines

    Parameters
    ----------
    arg1 : str
        The description of arg1
    arg2 : int
        The description of arg2

    Returns
    -------
    result : str
        The description of the result of the function

    Examples
    --------
    Have some really good examples here. And end paragraphs
    with a double colon to start 'code' sections::

    frame.function("a", 3)

    "aaa"

""")
    println(doc)
  }
}
