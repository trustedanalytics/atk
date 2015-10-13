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

package org.trustedanalytics.atk.model.publish.format

import java.io._
import org.apache.commons.compress.archivers.tar.{ TarArchiveEntry, TarArchiveInputStream }
import org.scalatest.WordSpec
import org.apache.commons.io.{ FileUtils, IOUtils }
import org.scalatest.Assertions._

class ModelPublishFormatTest extends WordSpec {

  "ModelPublishFormat" should {
    "create a tar of given files and place into an output stream" in {
      val myJar = File.createTempFile("test", ".jar")
      val filePath = myJar.getAbsolutePath
      val fileList = myJar :: myJar :: Nil
      var tarFile: File = null
      var tarOutput: FileOutputStream = null
      var counter = 0
      val modelReader = "org.trustedanalytics.atk.scoring.models.LdaModelReaderPlugin"
      val model = "This is a test Model"

      var myTarFileStream: TarArchiveInputStream = null
      try {
        tarFile = File.createTempFile("TestTar", ".tar")
        tarOutput = new FileOutputStream(tarFile)
        ModelPublishFormat.write(fileList, modelReader, model.getBytes, tarOutput)

        myTarFileStream = new TarArchiveInputStream(new FileInputStream(new File(tarFile.getAbsolutePath)))

        var entry = myTarFileStream.getNextTarEntry

        while (entry != null) {
          val individualFile = entry.getName
          val content = new Array[Byte](entry.getSize.toInt)
          myTarFileStream.read(content, 0, content.length)

          if (individualFile.contains(".jar")) {
            val fileName = filePath.substring(filePath.lastIndexOf("/") + 1)
            assert(individualFile.equals(fileName))
            counter = counter + 1
          }
          else if (individualFile.contains("modelReader")) {
            assert(new String(content).equals(modelReader))
          }
          else {
            assert(content.length == model.getBytes.length)
          }
          entry = myTarFileStream.getNextTarEntry
        }
        assert(counter == 2)
      }
      finally {
        FileUtils.deleteQuietly(tarFile)
        FileUtils.deleteQuietly(myJar)
      }
    }
  }

  //  "create a model given a tar file" in {
  //
  //  }
}

