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
import java.net.{ URL, URLClassLoader }
import org.trustedanalytics.atk.scoring.interfaces.{ ModelLoader, Model }
import org.apache.commons.compress.archivers.tar.{ TarArchiveInputStream, TarArchiveOutputStream, TarArchiveEntry }
import org.apache.commons.compress.archivers.ArchiveStreamFactory
import org.apache.commons.compress.archivers.ArchiveOutputStream
//import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils

/**
 * Read/write for publishing models
 */

object ModelPublishFormat {

  /**
   *  * Write a Model to a our special format that can be read later by a Scoring Engine.
   *    *
   *    * @param modelArchiveOutput location to store published model
   *    * @param classLoaderFiles list of jars and other files for ClassLoader
   *    * @param modelLoaderClassName class that implements the ModelLoader trait for instantiating the model during read()
   *    * @param modelData the trained model data
   *   
   */

  def write(classLoaderFiles: List[File], modelLoaderClass: String, modelData: Array[Byte], outputStream: FileOutputStream): Unit = {
    val myTarBall = new TarArchiveOutputStream(new BufferedOutputStream(outputStream))
    var modelDataFile: File = null
    var modelStream: DataOutputStream = null
    try {
      classLoaderFiles.foreach((file: File) => {
        val fileEntry = new TarArchiveEntry(file)
        fileEntry.setSize(file.length())
        myTarBall.putArchiveEntry(fileEntry)
        IOUtils.copy(new FileInputStream(file), myTarBall)
      })

      modelDataFile = File.createTempFile("ModelData", ".txt")
      modelStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(modelDataFile)))
      modelStream.write(modelData)

      var entryName = modelDataFile.getName
      var tarEntry: TarArchiveEntry = new TarArchiveEntry(modelDataFile, entryName)
      myTarBall.putArchiveEntry(tarEntry)
      IOUtils.copy(new FileInputStream(modelDataFile), myTarBall)
      myTarBall.closeArchiveEntry()

      val modelLoader = File.createTempFile("modelReader", ".txt")
      val classWriter: PrintWriter = new PrintWriter(modelLoader)
      classWriter.print(modelLoaderClass)
      classWriter.close()

      entryName = modelLoader.getName
      tarEntry = new TarArchiveEntry(modelLoader, entryName)
      myTarBall.putArchiveEntry(tarEntry)
      IOUtils.copy(new FileInputStream(modelLoader), myTarBall)
      myTarBall.closeArchiveEntry()
    }
    finally {
      myTarBall.finish()
      outputStream.close()
      modelStream.close()
      FileUtils.deleteQuietly(modelDataFile)
    }
  }

  /**
   * Read a Model from our special format using a private ClassLoader.
   *   
   * May throw exception if version of archive doesn't match current library.
   *    *
   *    * @param modelArchiveInput location to read published model from
   *    * @param parentClassLoader parentClassLoader to use for the private ClassLoader
   *    * @return the instantiated Model
   *   
   */
  def read(modelArchiveInput: File, parentClassLoader: ClassLoader): Model = {

    var outputFile: FileOutputStream = null
    var myTarFile: TarArchiveInputStream = null
    var modelName: String = null
    var ModelBytesFileName: String = null
    var archiveName: String = null
    var urls = Array.empty[URL]

    try {
      myTarFile = new TarArchiveInputStream(new FileInputStream(new File(modelArchiveInput.getAbsolutePath)))

      var entry: TarArchiveEntry = myTarFile.getNextTarEntry
      while (entry != null) {
        // Get the name of the file
        val individualFile: String = entry.getName
        // Get Size of the file and create a byte array for the size
        val content: Array[Byte] = new Array[Byte](entry.getSize.toInt)
        myTarFile.read(content, 0, content.length)
        outputFile = new FileOutputStream(new File(individualFile))
        IOUtils.write(content, outputFile)
        outputFile.close()
        if (individualFile.contains(".jar")) {
          val file = new File(individualFile)
          val url = file.toURI.toURL
          urls = urls :+ url
        }
        else if (individualFile.contains("modelname")) {
          val s = new String(content)
          modelName = s.replaceAll("\n", "")
        }
        else {
          ModelBytesFileName = individualFile
        }
        entry = myTarFile.getNextTarEntry
      }

      val source = scala.io.Source.fromFile(ModelBytesFileName)
      val byteArray = source.map(_.toByte).toArray
      source.close()

      val classLoader = new URLClassLoader(urls, parentClassLoader)
      val modelLoader = classLoader.loadClass(modelName).newInstance()

      modelLoader.asInstanceOf[ModelLoader].load(byteArray)
    }
    finally {
      IOUtils.closeQuietly(outputFile)
      IOUtils.closeQuietly(myTarFile)
    }

  }
}

