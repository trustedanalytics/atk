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

import java.io._
import java.lang.reflect.Field
import java.net.{ URL, URLClassLoader }
import java.nio.file.{ Path, Files }

import org.apache.commons.compress.archivers.tar.{ TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream }
import org.apache.commons.io.{ FileUtils, IOUtils }
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }

/**
 * Read/write for publishing models
 */

object ModelPublishFormat extends EventLogging {

  val modelDataString = "modelData"
  val modelReaderString = "modelReader"

  /**
   * Write a Model to a our special format that can be read later by a Scoring Engine.
   *   
   * @param classLoaderFiles list of jars and other files for ClassLoader
   * @param modelLoaderClass class that implements the ModelLoader trait for instantiating the model during read()
   * @param modelData the trained model data
   * @param outputStream location to store published model
   */
  def write(classLoaderFiles: List[File], modelLoaderClass: String, modelData: Array[Byte], outputStream: FileOutputStream): Unit = {
    val tarBall = new TarArchiveOutputStream(new BufferedOutputStream(outputStream))

    try {
      classLoaderFiles.foreach((file: File) => {
        if (!file.isDirectory && file.exists()) {
          addFileToTar(tarBall, file)
        }
      })

      addByteArrayToTar(tarBall, modelDataString + ".txt", modelData.length, modelData)
      addByteArrayToTar(tarBall, modelReaderString + ".txt", modelLoaderClass.length, modelLoaderClass.getBytes("utf-8"))
    }
    catch {
      case e: Exception =>
        error("writing model failed", exception = e)
        throw e
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
    var tarFile: TarArchiveInputStream = null
    var modelName: String = null
    var urls = Array.empty[URL]
    var byteArray: Array[Byte] = null
    var libraryPaths: Set[String] = Set.empty[String]

    try {
      // Extract files to temporary directory so that dynamic library names are not changed
      val tempDirectory = Files.createTempDirectory("tap-scoring-model")
      tarFile = new TarArchiveInputStream(new FileInputStream(modelArchiveInput))

      var entry = tarFile.getNextTarEntry
      while (entry != null) {
        val individualFile = entry.getName
        // Get Size of the file and create a byte array for the size
        val content = new Array[Byte](entry.getSize.toInt)
        tarFile.read(content, 0, content.length)

        if (individualFile.contains(".jar")) {
          val file = writeTempFile(tempDirectory, content, individualFile, ".jar")
          val url = file.toURI.toURL
          urls = urls :+ url
        }
        else if (individualFile.contains(".so")) {
          val file = writeTempFile(tempDirectory, content, individualFile, ".so")
          libraryPaths += getDirectoryPath(file)
        }
        else if (individualFile.contains(".dll")) {
          val file = writeTempFile(tempDirectory, content, individualFile, ".dll")
          libraryPaths += getDirectoryPath(file)
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

      addToJavaLibraryPath(libraryPaths) //Add temporary directory to java.library.path
      modelLoader.asInstanceOf[ModelLoader].load(byteArray)
    }
    catch {
      case e: Exception =>
        error("reading model failed", exception = e)
        throw e
    }
    finally {
      IOUtils.closeQuietly(tarFile)
    }

  }

  /**
   * Write content to temporary file
   *
   * @param tempDir Temporary directory
   * @param content File content to write
   * @param filePath File path
   * @param fileExtension File extension
   *
   * @return Temporary file
   */
  private def writeTempFile(tempDir: Path, content: Array[Byte], filePath: String, fileExtension: String): File = {
    var file: File = null
    var outputFile: FileOutputStream = null
    val fileName = filePath.substring(filePath.lastIndexOf("/") + 1)

    try {
      file = new File(tempDir.toString, fileName)
      file.createNewFile()
      //TODO: file.deleteOnExit()???
      outputFile = new FileOutputStream(file)
      IOUtils.write(content, outputFile)

    }
    catch {
      case e: Exception =>
        error(s"reading model failed due to error extracting file: ${filePath}", exception = e)
        throw e
    }
    finally {
      IOUtils.closeQuietly(outputFile)
    }
    file
  }

  /**
   * Add byte array contents to tar ball using
   *
   * @param tarBall Tar ball
   * @param entryName Name of entry to add
   * @param entrySize Size of entry
   * @param entryContent Content to add
   */
  private def addByteArrayToTar(tarBall: TarArchiveOutputStream, entryName: String, entrySize: Int, entryContent: Array[Byte]): Unit = {
    val modelEntry = new TarArchiveEntry(entryName)
    modelEntry.setSize(entrySize)
    tarBall.putArchiveEntry(modelEntry)
    IOUtils.copy(new ByteArrayInputStream(entryContent), tarBall)
    tarBall.closeArchiveEntry()
  }

  /**
   * Add file contents to tar ball using
   *
   * @param tarBall Tar ball
   * @param file File to add
   */
  private def addFileToTar(tarBall: TarArchiveOutputStream, file: File): Unit = {
    val fileEntry = new TarArchiveEntry(file, file.getName)
    tarBall.putArchiveEntry(fileEntry)
    IOUtils.copy(new FileInputStream(file), tarBall)
    tarBall.closeArchiveEntry()
  }

  /**
   * Get directory path for input file
   *
   * @param file Input file
   * @return Returns parent directory if input is a file, or absolute path if input is a directory
   */
  private def getDirectoryPath(file: File): String = {
    if (file.isDirectory) {
      file.getAbsolutePath
    }
    else {
      file.getParent
    }
  }

  /**
   * Dynamically add library paths to java.library.path
   *
   * @param libraryPaths Library paths to add
   */
  private def addToJavaLibraryPath(libraryPaths: Set[String]): Unit = {
    try {
      val usrPathField = classOf[ClassLoader].getDeclaredField("usr_paths")
      usrPathField.setAccessible(true)
      val newLibraryPaths = usrPathField.get(null).asInstanceOf[Array[String]].toSet ++ libraryPaths
      usrPathField.set(null, newLibraryPaths.toArray)
    }
    catch {
      case e: Exception =>
        error(s"reading model failed due to failure to set java.library.path: ${libraryPaths.mkString(",")}",
          exception = e)
        throw e
    }
  }

}

