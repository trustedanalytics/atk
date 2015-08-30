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

package org.trustedanalytics.atk.engine

import org.trustedanalytics.atk.domain.frame.{ FrameEntity, _ }
import org.trustedanalytics.atk.domain.schema.Schema
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.domain.CreateEntityArgs

trait FrameStorage {

  type Context
  type Data

  def expectFrame(frameRef: FrameReference)(implicit invocation: Invocation): FrameEntity

  @deprecated("please use expectFrame() instead")
  def lookup(id: Long)(implicit invocation: Invocation): Option[FrameEntity]
  def lookupByName(name: Option[String])(implicit invocation: Invocation): Option[FrameEntity]
  def getFrames()(implicit invocation: Invocation): Seq[FrameEntity]
  def create(arguments: CreateEntityArgs)(implicit invocation: Invocation): FrameEntity
  def renameFrame(frame: FrameEntity, newName: String)(implicit invocation: Invocation): FrameEntity
  def renameColumns(frame: FrameEntity, namePairs: Seq[(String, String)])(implicit invocation: Invocation): FrameEntity
  def getRows(frame: FrameEntity, offset: Long, count: Long)(implicit invocation: Invocation): Iterable[Array[Any]]
  def drop(frame: FrameEntity)(implicit invocation: Invocation)
  def loadFrameData(context: Context, frame: FrameEntity)(implicit invocation: Invocation): Data
  def saveFrameData(frame: FrameReference, data: Data)(implicit invocation: Invocation): FrameEntity

  def prepareForSave(frameEntity: FrameReference, storageFormat: Option[String] = None)(implicit invocation: Invocation): SaveInfo
  def postSave(targetFrameRef: FrameReference, saveInfo: SaveInfo, schema: Schema)(implicit invocation: Invocation): FrameEntity

  /**
   * Get the error frame of the supplied frame or create one if it doesn't exist
   * @param frame the 'good' frame
   * @return the 'error' frame associated with the 'good' frame
   */
  def lookupOrCreateErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): FrameEntity

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  def lookupErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): Option[FrameEntity]
  def sizeInBytes(frameEntity: FrameEntity)(implicit invocation: Invocation): Option[Long]

  def scheduleDeletion(frame: FrameEntity)(implicit invocation: Invocation): Unit
}

/**
 * Stores arguments which describe a saving a frame
 * @param targetPath HDFS path where the data is going to be saved, revision-specific
 * @param storageFormat storage format, like parquet
 * @param victimPath HDFS path which should be deleted after the save, like the previous revision folder
 */
case class SaveInfo(targetPath: String,
                    storageFormat: String,
                    victimPath: Option[String] = None)
