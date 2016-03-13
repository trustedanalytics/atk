/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.model.publish.format

import java.io._
import java.nio.charset.Charset
import org.apache.commons.compress.archivers.tar.{ TarArchiveOutputStream, TarArchiveEntry, TarArchiveInputStream }
import org.scalatest.WordSpec
import org.apache.commons.io.{ FileUtils, IOUtils }
import org.scalatest.Assertions._
import org.trustedanalytics.atk.scoring.interfaces.{ ModelMetaDataArgs, Model, Field }

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
        ModelPublishFormat.write(fileList, modelReader, model.getBytes(Charset.forName("utf-8")), tarOutput)

        myTarFileStream = new TarArchiveInputStream(new FileInputStream(new File(tarFile.getAbsolutePath)))

        var entry = myTarFileStream.getNextTarEntry

        while (entry != null) {
          val individualFile = entry.getName
          val content = new Array[Byte](entry.getSize.toInt)
          myTarFileStream.read(content, 0, content.length)

          if (individualFile.contains(".jar")) {
            counter = counter + 1
          }
          else if (individualFile.contains("modelReader")) {
            assert(new String(content).equals(modelReader))
          }
          else {
            assert(content.length == model.getBytes(Charset.forName("utf-8")).length)
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

  "create a model given a tar file" in {
    val testTarFile = File.createTempFile("TestTar", ".tar")
    val testJar = File.createTempFile("test", ".jar")
    val testTarBall = new TarArchiveOutputStream(new BufferedOutputStream(new FileOutputStream(testTarFile)))
    var modelDataFile = File.createTempFile("modelData", ".txt")
    var modelLoaderFile = File.createTempFile("modelReader", ".txt")

    def writeEntry(file: File): Unit =
      {
        val fileEntry = new TarArchiveEntry(file)
        testTarBall.putArchiveEntry(fileEntry)
        IOUtils.copy(new FileInputStream(file), testTarBall)
        testTarBall.closeArchiveEntry()
      }

    try {
      val entryName = testJar.getName
      val fileEntry = new TarArchiveEntry(testJar, entryName)
      fileEntry.setSize(testJar.length())
      testTarBall.putArchiveEntry(fileEntry)
      IOUtils.copy(new FileInputStream(testJar), testTarBall)
      testTarBall.closeArchiveEntry()

      FileUtils.writeByteArrayToFile(modelDataFile, "This is a test model data".getBytes(Charset.forName("utf-8")))
      writeEntry(modelDataFile)

      FileUtils.writeStringToFile(modelLoaderFile, "org.trustedanalytics.atk.model.publish.format.TestModelReaderPlugin")
      writeEntry(modelLoaderFile)
      testTarBall.finish()

      val testModel = ModelPublishFormat.read(testTarFile, this.getClass.getClassLoader)

      assert(testModel.isInstanceOf[Model])
      assert(testModel != null)
    }
    finally {

      IOUtils.closeQuietly(testTarBall)
      FileUtils.deleteQuietly(modelLoaderFile)
      FileUtils.deleteQuietly(modelDataFile)
      FileUtils.deleteQuietly(testTarFile)
      FileUtils.deleteQuietly(testJar)
    }
  }
}
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }

class TestModelReaderPlugin extends ModelLoader {

  private var testModel: TestModel = _

  override def load(bytes: Array[Byte]): Model = {
    testModel = new TestModel
    testModel.asInstanceOf[Model]
  }
}

class TestModel() extends Model {

  override def score(data: Array[Any]): Array[Any] = {
    var score = Array[Any]()
    score = score :+ 2
    score
  }

  override def input: Array[Field] = {
    var input = Array[Field]()
    input = input :+ Field("input", "Float")
    input
  }

  override def output: Array[Field] = {
    var output = Array[Field]()
    output = output :+ Field("output", "Float")
    output
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("Dummy Model", "dummy class", "dummy reader", Map("created_on" -> "Jan 29th 2016"))
  }
}

