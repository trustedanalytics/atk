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
package org.trustedanalytics.atk.engine.model.plugins.scoring

import java.io._
import java.net.URI
import java.util.UUID

import org.apache.commons.compress.archivers.tar.{ TarArchiveEntry, TarArchiveOutputStream }
import org.apache.commons.io.{ FileUtils, IOUtils }
import java.io.File
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.{ EngineConfig, FileStorage }
import org.trustedanalytics.atk.engine.plugin.ArgDoc
import org.trustedanalytics.atk.moduleloader.Module
import org.trustedanalytics.atk
import org.trustedanalytics.atk.model.publish.format.ModelPublishFormat

object ModelPublishJsonProtocol {

  implicit val modelPublishFormat = jsonFormat1(ModelPublishArgs)

}

case class ModelPublishArgs(model: ModelReference) {
  require(model != null, "model is required")
}

case class ModelPublishArtifact(filePath: String, fileSize: Long) {
  require(StringUtils.isNotEmpty(filePath), "published path for model artifact should not empty")
  require(fileSize > 0, "published artifact size should be greater than 0")
}

object ModelPublish {

  def createTarForScoringEngine(modelData: Array[Byte], scoringModelJar: String, modelClassName: String): ModelPublishArtifact = {

    var tarFile: File = null
    var tarOutput: FileOutputStream = null

    try {
      val fileList = Module.allLibs("scoring-models").map(jarUrl => new File(jarUrl.getPath)).toList

      tarFile = File.createTempFile("modelTar", ".tar")
      tarOutput = new FileOutputStream(tarFile)

      ModelPublishFormat.write(fileList, modelClassName, modelData, tarOutput)

      val fileStorage = new FileStorage
      val tarFileName = fileStorage.absolutePath("models_" + UUID.randomUUID().toString.replaceAll("-", "") + ".tar").toString
      val hdfsPath = new Path(tarFileName)
      val hdfsFileSystem: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new URI(tarFileName), new Configuration())
      val localPath = new Path(tarFile.getAbsolutePath)
      hdfsFileSystem.copyFromLocalFile(false, true, localPath, hdfsPath)
      ModelPublishArtifact(tarFileName, hdfsFileSystem.getContentSummary(hdfsPath).getLength)
    }
    finally {
      FileUtils.deleteQuietly(tarFile)
      IOUtils.closeQuietly(tarOutput)
    }
  }
}
