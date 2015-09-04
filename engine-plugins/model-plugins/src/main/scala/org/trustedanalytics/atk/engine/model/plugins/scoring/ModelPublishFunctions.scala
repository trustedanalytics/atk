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

package org.trustedanalytics.atk.engine.model.plugins.scoring

import java.io._
import java.net.URI
import java.util.UUID

import org.trustedanalytics.atk.component.Archive
import org.apache.commons.compress.archivers.tar.{ TarArchiveEntry, TarArchiveOutputStream }
import org.apache.commons.io.IOUtils
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.{ EngineConfig, HdfsFileStorage }
import org.trustedanalytics.atk.engine.plugin.ArgDoc

object ModelPublishJsonProtocol {

  implicit val modelPublishFormat = jsonFormat1(ModelPublishArgs)

}

case class ModelPublishArgs(model: ModelReference) {
  require(model != null, "model is required")
}

object ModelPublish {

  def createTarForScoringEngine(modelData: String, scoringModelJar: String, modelClassName: String): String = {

    var tOut: TarArchiveOutputStream = null

    try {
      val modelDatafile = new File("/tmp/modelbytes")
      // if file doesnt exists, then create it
      if (!modelDatafile.exists()) {
        modelDatafile.createNewFile()
      }
      val writer: PrintWriter = new PrintWriter(modelDatafile)
      writer.print(modelData)
      writer.close()

      val modelClassNamefile = new File("/tmp/" + "modelname.txt")
      // if file doesnt exists, then create it
      if (!modelClassNamefile.exists()) {
        modelDatafile.createNewFile()
      }
      val classWriter: PrintWriter = new PrintWriter(modelClassNamefile)
      classWriter.print(modelClassName)
      classWriter.close()

      val jarFile = new File(Archive.getJar(scoringModelJar).toString.substring(5)) //remove the first 5 characeters of the file location

      val tarTempPath = "/tmp/scoring.tar"
      val tarTempFile = new File(tarTempPath)
      if (tarTempFile.exists()) {
        tarTempFile.delete()
      }
      tarTempFile.createNewFile()

      tOut = new TarArchiveOutputStream(new BufferedOutputStream(new FileOutputStream(tarTempFile)))

      var entryName = modelDatafile.getName
      var tarEntry: TarArchiveEntry = new TarArchiveEntry(modelDatafile, entryName)
      tOut.putArchiveEntry(tarEntry)
      IOUtils.copy(new FileInputStream(modelDatafile), tOut)
      tOut.closeArchiveEntry()

      entryName = modelClassNamefile.getName
      tarEntry = new TarArchiveEntry(modelClassNamefile, entryName)
      tOut.putArchiveEntry(tarEntry)
      IOUtils.copy(new FileInputStream(modelClassNamefile), tOut)
      tOut.closeArchiveEntry()

      entryName = jarFile.getName
      tarEntry = new TarArchiveEntry(jarFile, entryName)
      tOut.putArchiveEntry(tarEntry)
      IOUtils.copy(new FileInputStream(jarFile), tOut)
      tOut.closeArchiveEntry()
      tOut.finish()

      val localPath = new Path(tarTempPath)
      val fileStorage = new HdfsFileStorage
      val tarFileName = fileStorage.absolutePath("models_" + UUID.randomUUID().toString.replaceAll("-", "") + ".tar").toString
      val hdfsPath = new Path(tarFileName)
      val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(tarFileName), new Configuration())
      hdfsFileSystem.copyFromLocalFile(false, true, localPath, hdfsPath)
      tarFileName
    }
    finally {
      IOUtils.closeQuietly(tOut)
    }
  }
}
