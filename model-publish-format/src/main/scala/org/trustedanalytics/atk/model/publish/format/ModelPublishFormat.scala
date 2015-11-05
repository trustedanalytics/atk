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

package org.trustedanalytics.atk.model.publish.format

import java.io
import java.io._
import java.net.{ URL, URLClassLoader }
import javax.crypto.KeyGenerator
import org.trustedanalytics.atk.scoring.interfaces.{ ModelLoader, Model }
import org.apache.commons.compress.archivers.tar.{ TarArchiveInputStream, TarArchiveOutputStream, TarArchiveEntry }
import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils

/**
 * Read/write for publishing models
 */

object ModelPublishFormat {

  /**
   *  Write a Model to a our special format that can be read later by a Scoring Engine.
   *   
   *  @param outputStream location to store published model
   *  @param classLoaderFiles list of jars and other files for ClassLoader
   *  @param modelLoaderClass class that implements the ModelLoader trait for instantiating the model during read()
   *  @param modelData the trained model data
   *   
   */

  val modelDataString = "modelData"
  val modelReaderString = "modelReader"

  def write(classLoaderFiles: List[File], modelLoaderClass: String, modelData: Array[Byte], outputStream: FileOutputStream): Unit = {
    val tarBall = new TarArchiveOutputStream(new BufferedOutputStream(outputStream))

    try {
      classLoaderFiles.foreach((file: File) => {
        if (!file.isDirectory && file.exists()) {
          val fileEntry = new TarArchiveEntry(file, file.getName)
          tarBall.putArchiveEntry(fileEntry)
          IOUtils.copy(new FileInputStream(file), tarBall)
          tarBall.closeArchiveEntry()
        }
      })

      val modelDataEntry = new TarArchiveEntry(modelDataString + ".txt")
      modelDataEntry.setSize(modelData.length)
      tarBall.putArchiveEntry(modelDataEntry)
      IOUtils.copy(new ByteArrayInputStream(modelData), tarBall)
      tarBall.closeArchiveEntry()

      val modelLoaderEntry = new TarArchiveEntry(modelReaderString + ".txt")
      modelLoaderEntry.setSize(modelLoaderClass.length)
      tarBall.putArchiveEntry(modelLoaderEntry)
      IOUtils.copy(new ByteArrayInputStream(modelLoaderClass.getBytes("utf-8")), tarBall)
      tarBall.closeArchiveEntry()
    }
    finally {
      tarBall.finish()
      IOUtils.closeQuietly(tarBall)
    }
  }

  /**
   * Read a Model from our special format using a private ClassLoader.
   *   
   * May throw exception if version of archive doesn't match current library.
   *
   * @param modelArchiveInput location to read published model from
   * @param parentClassLoader parentClassLoader to use for the private ClassLoader
   * @return the instantiated Model   
   */
  def read(modelArchiveInput: File, parentClassLoader: ClassLoader): Model = {

    var outputFile: FileOutputStream = null
    var tarFile: TarArchiveInputStream = null
    var modelName: String = null
    var ModelBytesFileName: String = null
    var archiveName: String = null
    var urls = Array.empty[URL]
    var byteArray: Array[Byte] = null
    var file: File = null

    try {
      tarFile = new TarArchiveInputStream(new FileInputStream(modelArchiveInput))

      var entry = tarFile.getNextTarEntry
      while (entry != null) {
        val individualFile = entry.getName
        // Get Size of the file and create a byte array for the size
        val content = new Array[Byte](entry.getSize.toInt)
        tarFile.read(content, 0, content.length)

        if (individualFile.contains(".jar")) {
          var fileName = individualFile.substring(individualFile.lastIndexOf("/") + 1)
          fileName = fileName.substring(0, fileName.length - 4)
          file = File.createTempFile(fileName, ".jar")
          outputFile = new FileOutputStream(file)
          IOUtils.write(content, outputFile)

          val url = file.toURI.toURL
          urls = urls :+ url
        }
        else if (individualFile.contains(modelReaderString)) {
          val s = new String(content)
          modelName = s.replaceAll("\n", "")
        }
        else if (individualFile.contains(modelDataString)) {
          byteArray = content
        }
        entry = tarFile.getNextTarEntry
      }

      val classLoader = new URLClassLoader(urls, parentClassLoader)
      val modelLoader = classLoader.loadClass(modelName).newInstance()

      modelLoader.asInstanceOf[ModelLoader].load(byteArray)
    }
    finally {
      IOUtils.closeQuietly(outputFile)
      IOUtils.closeQuietly(tarFile)
      FileUtils.deleteQuietly(file)
    }

  }
}

