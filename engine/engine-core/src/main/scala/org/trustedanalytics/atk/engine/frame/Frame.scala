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

package org.trustedanalytics.atk.engine.frame

import org.apache.spark.sql.{ SQLContext, DataFrame }
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.schema.Schema
import org.trustedanalytics.atk.engine.FrameStorage
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation }
import org.apache.spark.SparkContext
import org.apache.spark.frame.FrameRdd

/**
 * Interface for Plugin Authors for interacting with Frames.
 *
 * Implicit conversion can be used to convert between a FrameReference and a Frame.
 *
 * Also see SparkFrame
 */
trait Frame {

  /**
   * The entity is the meta data representation for a Frame that we store in the DB.
   *
   * Ideally, plugin authors wouldn't work directly with entities.
   */
  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  def entity: FrameEntity

  /** name assigned by user */
  def name: Option[String]

  def name_=(updatedName: String): Unit

  /** the schema of the frame (defines columns, data types, etc) */
  def schema: Schema

  /** Rename columns supplying old and new names */
  def renameColumns(namePairs: Seq[(String, String)])

  /**
   * lifecycle status. For example, ACTIVE, DELETED (un-delete possible), DELETE_FINAL (no un-delete)
   */
  def status: Long

  /** description of frame (a good default description might be the name of the input file) */
  def description: Option[String]

  /** number of rows in the frame, None if unknown or Frame has not been materialized */
  def rowCount: Option[Long]

  def sizeInBytes: Option[Long]
}

object Frame {

  implicit def frameToFrameEntity(frame: Frame): FrameEntity = frame.entity

  implicit def frameToFrameReference(frame: Frame): FrameReference = frame.entity.toReference
}

/**
 * Interface for Plugin Authors for interacting with Frames in Spark runtime.
 *
 * Implicit conversion can be used to convert between a FrameReference and a SparkFrame.
 */
trait SparkFrame extends Frame {

  /** Load the frame's data as an RDD */
  def rdd: FrameRdd

  /** Update the data for this frame */
  def save(rdd: FrameRdd): SparkFrame

  /** Convert to a Spark DataFrame */
  def toDataFrame(): DataFrame
}

object SparkFrame {

  implicit def sparkFrameToFrameEntity(sparkFrame: SparkFrame): FrameEntity = sparkFrame.entity

  implicit def sparkFrameToFrameReference(sparkFrame: SparkFrame): FrameReference = sparkFrame.entity.toReference
}

/**
 * Actual implementation for Frame interface
 */
class FrameImpl(frame: FrameReference, frameStorage: FrameStorage)(implicit invocation: Invocation) extends Frame {

  /**
   * The entity is the meta data representation for a Frame that we store in the DB.
   *
   * Ideally, plugin authors wouldn't work directly with entities.
   */
  @deprecated("use other methods in interface, we want to move away from exposing entities to plugin authors")
  override def entity: FrameEntity = frameStorage.expectFrame(frame)

  override def description: Option[String] = entity.description

  /** name assigned by user */
  override def name: Option[String] = entity.name

  override def name_=(updatedName: String): Unit = frameStorage.renameFrame(entity, updatedName)

  /** number of rows in the frame, None if unknown or Frame has not been materialized */
  override def rowCount: Option[Long] = entity.rowCount

  /**
   * lifecycle status. For example, ACTIVE, DELETED (un-delete possible), DELETE_FINAL (no un-delete)
   */
  override def status: Long = entity.status

  /** the schema of the frame (defines columns, data types, etc) */
  override def schema: Schema = entity.schema

  override def sizeInBytes: Option[Long] = frameStorage.sizeInBytes(entity)

  /** Rename columns supplying old and new names */
  override def renameColumns(namePairs: Seq[(String, String)]): Unit = {
    frameStorage.renameColumns(entity, namePairs)
  }
}

/**
 * Actual implementation for SparkFrame interface
 */
class SparkFrameImpl(frame: FrameReference, sc: SparkContext, sparkFrameStorage: SparkFrameStorage)(implicit invocation: Invocation)
    extends FrameImpl(frame, sparkFrameStorage) with SparkFrame {

  /** Load the frame's data as an RDD */
  override def rdd: FrameRdd = sparkFrameStorage.loadFrameData(sc, entity)

  /** Update the data for this frame */
  override def save(rdd: FrameRdd): SparkFrame = {
    val result = sparkFrameStorage.saveFrameData(frame, rdd)
    new SparkFrameImpl(result, sc, sparkFrameStorage)
  }

  /** Convert to a Spark DataFrame */
  override def toDataFrame(): DataFrame = {
    this.rdd.toDataFrame
  }

}
