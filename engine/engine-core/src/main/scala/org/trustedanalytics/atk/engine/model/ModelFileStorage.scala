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

package org.trustedanalytics.atk.engine.model

import java.io.{ OutputStream, InputStream }

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.permission.{ FsAction, FsPermission }
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.trustedanalytics.atk.EventLoggingImplicits
import org.trustedanalytics.atk.domain.model.ModelEntity
import org.trustedanalytics.atk.engine.{ EntityRev, SaveInfo, FileStorage }
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.event.EventLogging
import spray.json.{ JsonParser, JsObject }

/**
 * Model storage in HDFS.
 *
 * @param fsRoot root for our application, e.g. "hdfs://hostname/user/atkuser"
 * @param hdfs methods for interacting with underlying storage (e.g. HDFS)
 */
class ModelFileStorage(fsRoot: String,
                       val hdfs: FileStorage)(implicit startupInvocation: Invocation)
    extends EventLogging with EventLoggingImplicits {

  private val modelsBaseDirectory = new Path(fsRoot + "/trustedanalytics/models")

  withContext("ModelFileStorage") {
    info("fsRoot: " + fsRoot)
    info("model base directory: " + modelsBaseDirectory)
    if (hdfs != null && hdfs.hdfs.exists(modelsBaseDirectory) == false) {
      FileSystem.mkdirs(hdfs.hdfs, modelsBaseDirectory, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE))
    }
  }

  /**
   * Determines what the storage path should be based on information in the Entity and current configuration
   * @param model the model to act on
   * @return
   */
  def calculateModelPath(model: ModelEntity): Path = {
    new Path(modelsBaseDirectory, model.id.toString)
  }

  /**
   * Remove the directory and underlying data for a particular modle
   * @param model the data model to act on
   */
  def deleteModelData(model: ModelEntity): Unit = {
    getModelFolder(model) match {
      case Some(path) => if (hdfs.exists(path)) { deletePath(path) }
      case _ =>
    }
  }

  /**
   * Remove the directory and underlying data for a particular model
   * @param path the path of the dir to remove
   */
  def deletePath(path: Path): Unit = {
    hdfs.delete(path, recursive = true)
  }

  def readJsObject(path: Path): JsObject = {
    var in: InputStream = null
    try {
      in = hdfs.read(path)
      readJsObject(in)
    }
    finally {
      IOUtils.closeQuietly(in)
    }
  }

  def writeJsObject(path: Path, jsObject: JsObject): Unit = {
    var out: OutputStream = null
    try {
      out = hdfs.write(path, append = false)
      writeJsObject(out, jsObject)
    }
    finally {
      IOUtils.closeQuietly(out)
    }
  }

  def readJsObject(in: InputStream): JsObject = {
    JsonParser(scala.io.Source.fromInputStream(in).getLines().mkString("")).asJsObject
  }

  def writeJsObject(out: OutputStream, obj: JsObject): Unit = {
    out.write(obj.compactPrint.getBytes)
  }

  //  folder structure:
  //
  //  */models/                  # entity collection folder
  //  */models/3/                # model folder
  //  */models/3/r2/             # model rev folder
  //  */models/3/r2/data.json    # model data file path

  /** gets the model folder of the given entity, if it exists */
  def getModelFolder(model: ModelEntity): Option[Path] = {
    getModelRevFolder(model) match {
      case Some(p) => Some(p.getParent)
      case None => None
    }
  }

  /** gets the current model rev folder of the given entity, if it exists */
  def getModelRevFolder(model: ModelEntity): Option[Path] = {
    model.storageLocation match {
      case Some(s) => Some(new Path(s).getParent)
      case None => None
    }
  }

  /** adds the data file name to the given model rev folder path */
  def addStorageLocationFileName(modelRevFolder: Path): Path = new Path(modelRevFolder, "data.json")

  /** returns the file path for the next rev data (creates necessary folders) */
  def prepareStorageLocationForNextRev(model: ModelEntity): Path = {
    val nextRevFolder = prepareModelRevFolderForNextRev(model)
    addStorageLocationFileName(nextRevFolder)
  }

  /**
   * (helper) creates a fresh folder for the model's next rev
   * @return path of the new model rev folder
   */
  private def prepareModelRevFolderForNextRev(model: ModelEntity): Path = {
    val modelFolder = calculateModelPath(model)
    val revFolder = getModelRevFolder(model)
    if (revFolder.isEmpty && hdfs.exists(modelFolder)) {
      deletePath(modelFolder) // delete the full folder to remove any pre-existing old data
    }
    hdfs.createDirectory(modelFolder)
    val nextRevFolder = new Path(modelFolder, EntityRev.getNextRevFolderName(revFolder.map(_.toString)))
    deletePath(nextRevFolder) // delete incomplete data on disk if it exists
    nextRevFolder
  }
}
