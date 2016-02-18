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

package org.trustedanalytics.atk.moduleloader.internal

import java.io.{ ByteArrayInputStream, File }

import org.scalatest.WordSpec

class ZipUtilsTest extends WordSpec {

  "ZipUtils" should {

    "read all files from a zip" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = ZipUtils.readFilesFromZip(zip, Seq("one.txt", "two.txt", "three.txt"))
      assert(results == Seq("one", "two", "three"))
    }

    "read some files from a zip" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = ZipUtils.readFilesFromZip(zip, Seq("one.txt", "two.txt"))
      assert(results == Seq("one", "two"))
    }

    "read multiple files from a zip and treat non-existent files as empty" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = ZipUtils.readFilesFromZip(zip, Seq("one.txt", "two.txt", "does-not-exist"))
      assert(results == Seq("one", "two", ""))
    }

    "convert empty stream to empty String" in {
      assert("" == ZipUtils.convertStreamToString(new ByteArrayInputStream(new Array[Byte](0))))
    }

    "convert stream to String" in {
      val str = "abcdefg"
      assert(str == ZipUtils.convertStreamToString(new ByteArrayInputStream(str.getBytes)))
    }
  }
}
