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

class FileUtilsTest extends WordSpec {

  "FileUtils" should {

    "be able to read files to string" in {
      val file = new File("src/test/resources/fileutils-test-data/directory-with-3-files/one.txt")
      assert("one" == FileUtils.readFileToString(file))
    }

    "read non-existent file as empty string" in {
      assert("" == FileUtils.readFileToString(new File("src/test/resources/does-not-exist.txt")))
    }

    "read all files from a directory" in {
      val directory = new File("src/test/resources/fileutils-test-data/directory-with-3-files")
      val results = FileUtils.readFilesFromDirectory(directory, Seq("one.txt", "two.txt", "three.txt"))
      assert(results == Seq("one", "two", "three"))
    }

    "read some files from a directory" in {
      val directory = new File("src/test/resources/fileutils-test-data/directory-with-3-files")
      val results = FileUtils.readFilesFromDirectory(directory, Seq("one.txt", "two.txt"))
      assert(results == Seq("one", "two"))
    }

    "read multiple files from a directory and treat non-existent files as empty" in {
      val directory = new File("src/test/resources/fileutils-test-data/directory-with-3-files")
      val results = FileUtils.readFilesFromDirectory(directory, Seq("one.txt", "two.txt", "does-not-exist"))
      assert(results == Seq("one", "two", ""))
    }

    "read all files from a zip" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = FileUtils.readFilesFromZip(zip, Seq("one.txt", "two.txt", "three.txt"))
      assert(results == Seq("one", "two", "three"))
    }

    "read some files from a zip" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = FileUtils.readFilesFromZip(zip, Seq("one.txt", "two.txt"))
      assert(results == Seq("one", "two"))
    }

    "read multiple files from a zip and treat non-existent files as empty" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = FileUtils.readFilesFromZip(zip, Seq("one.txt", "two.txt", "does-not-exist"))
      assert(results == Seq("one", "two", ""))
    }

    "convert empty stream to empty String" in {
      assert("" == FileUtils.convertStreamToString(new ByteArrayInputStream(new Array[Byte](0))))
    }

    "convert stream to String" in {
      val str = "abcdefg"
      assert(str == FileUtils.convertStreamToString(new ByteArrayInputStream(str.getBytes)))
    }

    "read all files from both directories and zip files (zip case)" in {
      val zip = new File("src/test/resources/fileutils-test-data/zip-with-3-files.zip")
      val results = FileUtils.readFiles(zip, Seq("one.txt", "two.txt", "three.txt"))
      assert(results == Seq("one", "two", "three"))
    }

    "read all files from both directories and zip files (directory case)" in {
      val directory = new File("src/test/resources/fileutils-test-data/directory-with-3-files")
      val results = FileUtils.readFiles(directory, Seq("one.txt", "two.txt", "three.txt"))
      assert(results == Seq("one", "two", "three"))
    }
  }
}
