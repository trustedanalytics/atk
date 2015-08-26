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

package org.trustedanalytics.atk.engine.frame

import org.trustedanalytics.atk.domain.schema.Schema
import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.component.ClassLoaderAware
import org.trustedanalytics.atk.domain.frame.{ FrameReference, DataFrameTemplate, FrameEntity }
import org.trustedanalytics.atk.engine.FrameStorage
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.engine.frame.parquet.ParquetReader
import org.trustedanalytics.atk.engine.partitioners.SparkAutoPartitioner
import org.trustedanalytics.atk.repository.SlickMetaStoreComponent
import org.trustedanalytics.atk.{ EventLoggingImplicits, DuplicateNameException, NotFoundException }
import org.apache.hadoop.fs.Path
import org.apache.spark.frame.FrameRdd
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.trustedanalytics.atk.event.EventLogging
import org.joda.time.DateTime
import scala.util.{ Failure, Success, Try }
import scala.util.control.NonFatal

import scala.language.implicitConversions

class SparkFrameStorage(val frameFileStorage: FrameFileStorage,
                        maxRows: Int,
                        val metaStore: SlickMetaStoreComponent#SlickMetaStore,
                        sparkAutoPartitioner: SparkAutoPartitioner)
    extends FrameStorage
    with EventLogging
    with EventLoggingImplicits
    with ClassLoaderAware {

  override type Context = SparkContext
  override type Data = FrameRdd

  val defaultStorageFormat: String = StorageFormats.FileParquet // todo: add to config

  /**
   * Copy the frames using spark
   *
   * @param frames List of frames to be copied
   * @param sc Spark Context
   * @return List of copied frames
   */
  def copyFrames(frames: List[FrameReference], sc: SparkContext)(implicit invocation: Invocation): List[FrameEntity] = {
    frames.map(frame => copyFrame(frame, sc))
  }

  /**
   * Create a copy of the frame copying the data
   * @param frame the frame to be copied
   * @return the copy
   */
  def copyFrame(frame: FrameReference, sc: SparkContext)(implicit invocation: Invocation): FrameEntity = {
    val frameEntity = expectFrame(frame)
    var child: FrameEntity = null

    metaStore.withTransaction("sfs.copyFrame") { implicit txn =>
      child = frameEntity.createChild(Some(invocation.user.user.id), command = None, schema = frameEntity.schema)
      child = metaStore.frameRepo.insert(child)
    }
    //TODO: frameEntity should just have a pointer to the actual frame data so that we don't have to load into Spark.
    val frameRdd = loadFrameData(sc, frameEntity)
    saveFrameData(child.toReference, frameRdd)

    expectFrame(child.toReference)
  }

  import org.apache.spark.sql.Row

  override def expectFrame(frameRef: FrameReference)(implicit invocation: Invocation): FrameEntity = {
    lookup(frameRef.frameId).getOrElse(throw new NotFoundException("frame", frameRef.frameId))
  }

  /**
   * Create an FrameRdd from a frame data file.
   *
   * This is our preferred format for loading frames as RDDs.
   *
   * @param sc spark context
   * @param frame the model for the frame
   * @return the newly created FrameRdd
   */
  def loadFrameData(sc: SparkContext, frame: FrameEntity)(implicit invocation: Invocation): FrameRdd = withContext("loadFrameRdd") {
    (frame.storageFormat, frame.storageLocation) match {
      case (_, None) | (None, _) =>
        //  nothing has been saved to disk yet)
        new FrameRdd(frame.schema, sc.parallelize[Row](Nil, EngineConfig.minPartitions))
      case (_, _) if frame.rowCount == Some(0L) =>
        new FrameRdd(frame.schema, sc.parallelize[Row](Nil, EngineConfig.minPartitions))
      case (Some("file/parquet"), Some(absPath)) =>
        val sqlContext = new SQLContext(sc)
        val parquetReader = new ParquetReader(new Path(absPath), frameFileStorage.hdfs)
        val paths = parquetReader.files().map(path => path.toUri.toString).toSeq
        val rows = sqlContext.parquetFile(paths: _*)
        val frameRdd = new FrameRdd(frame.schema, rows)
        sparkAutoPartitioner.repartitionFromFileSize(absPath.toString, frameRdd)
      case (Some(s), _) => illegalArg(s"Cannot load frame with storage '$s'")
    }
  }

  /**
   * Determine if a dataFrame is saved as parquet
   * @param frame the data frame to verify
   * @return true if the data frame is saved in the parquet format
   */
  def isParquet(frame: FrameEntity): Boolean = {
    frameFileStorage.isParquet(frame)
  }

  /**
   * Save a FrameRdd to HDFS.
   *
   * This is our preferred path for saving RDDs as data frames.
   *
   * If the current frame is already materialized, a new entry will be created in the meta data repository, otherwise the existing entry will be updated.
   *
   * @param frame reference to a data frame
   * @param frameRdd the RDD containing the actual data
   */
  override def saveFrameData(frame: FrameReference, frameRdd: FrameRdd)(implicit invocation: Invocation): FrameEntity =
    withContext("SFS.saveFrame") {
      val saveInfo = prepareForSave(frame)
      frameRdd.save(saveInfo.targetPath, saveInfo.storageFormat)
      postSave(frame, saveInfo, frameRdd.frameSchema)
    }

  // The implemented mutable strategy increments a subdirectory for a given
  // frame path.  The given path is the source and path+1 is the destination.
  val revPattern = """.*r(\d+)$""".r

  /**
   * Gets the next revision's folder name
   * @param frame entity to be saved
   * @return folder name (not absolute)
   */
  private def getNextRevFolderName(frame: FrameEntity): String = {
    val r = frame.storageLocation match {
      case Some(path) => path match {
        case revPattern(number) => number.toInt + 1
      }
      case None => 0
    }
    s"r$r"
  }

  /**
   * Prepare save path, return info about the save
   *
   * Exposed here for Giraph (and anything else where the frame will be materialized outside of Spark)
   * - you don't need to call this for Spark plugins.
   *
   * Developer needs to call postSave() once the frame has been materialized.
   */
  def prepareForSave(frame: FrameReference, forceStorageFormat: Option[String] = None)(implicit invocation: Invocation): SaveInfo = {
    val frameEntity = expectFrame(frame)
    val targetPath = new Path(frameFileStorage.calculateFramePath(frameEntity), getNextRevFolderName(frameEntity))
    // delete incomplete data on disk if it exists
    frameFileStorage.deletePath(targetPath)
    val storageFormat = forceStorageFormat.getOrElse(frameEntity.storageFormat.getOrElse(defaultStorageFormat))
    info(s"Preparing to save frame ${frame.id} (name: ${frameEntity.name}) to $targetPath as $storageFormat")
    SaveInfo(targetPath = targetPath.toString, victimPath = frameEntity.storageLocation, storageFormat = storageFormat)
  }

  /**
   * After saving update timestamps, status, row count, etc.
   *
   * Exposed here for Giraph - you don't need to call this for Spark plugins.
   *
   * @param targetFrameRef might be same as originalFrameRef or the next revision
   * @param saveInfo prepare save info
   * @param schema the new schema
   * @return the latest version of the entity
   */
  override def postSave(targetFrameRef: FrameReference, saveInfo: SaveInfo, schema: Schema)(implicit invocation: Invocation): FrameEntity = {
    // update the metastore
    metaStore.withSession("frame.saveFrame") {
      implicit session =>
        {
          val frameEntity = expectFrame(targetFrameRef)

          require(schema != null, "frame schema was null, we need developer to add logic to handle this - we used to have this, not sure if we still do --Todd 12/16/2014")

          val updatedFrame = frameEntity.copy(status = Status.Active,
            schema = schema,
            storageLocation = Some(saveInfo.targetPath),
            storageFormat = Some(saveInfo.storageFormat),
            materializationComplete = Some(new DateTime),
            modifiedOn = new DateTime,
            modifiedBy = Some(invocation.user.user.id))

          val withCount = updatedFrame.copy(rowCount = Some(getRowCount(updatedFrame)))

          metaStore.frameRepo.update(withCount)

          if (saveInfo.victimPath.isDefined) {
            frameFileStorage.deletePath(new Path(saveInfo.victimPath.get))
          }
        }
    }
    expectFrame(targetFrameRef)
  }

  /**
   * Retrieve records from the given dataframe
   * @param frame Frame to retrieve records from
   * @param offset offset in frame before retrieval
   * @param count number of records to retrieve
   * @return records in the dataframe starting from offset with a length of count
   */
  override def getRows(frame: FrameEntity, offset: Long, count: Long)(implicit invocation: Invocation): Iterable[Array[Any]] =
    withContext("frame.getRows") {
      require(frame != null, "frame is required")
      require(offset >= 0, "offset must be zero or greater")
      require(count > 0, "count must be zero or greater")
      withMyClassLoader {
        val reader = getReader(frame)
        val rows = reader.take(count, offset, Some(maxRows))
        metaStore.withSession("frame.updateFrameStatus") {
          implicit session =>
            {
              metaStore.frameRepo.updateLastReadDate(frame)
            }
        }
        rows
      }
    }

  /**
   * Row count for the supplied frame (assumes Parquet storage)
   * @return row count
   */
  def getRowCount(frame: FrameEntity)(implicit invocation: Invocation): Long = {
    if (frame.storageLocation.isDefined) {
      val reader = getReader(frame)
      reader.rowCount()
    }
    else {
      // TODO: not sure what to do if nothing is persisted?
      throw new NotImplementedError("trying to get a row count on a frame that hasn't been persisted, not sure what to do")
    }
  }

  /**
   * Size of frame in bytes
   *
   * @param frameEntity reference to a data frame
   * @return Optional size of frame in bytes
   */
  def sizeInBytes(frameEntity: FrameEntity)(implicit invocation: Invocation): Option[Long] = {
    (frameEntity.storageFormat, frameEntity.storageLocation) match {
      case (Some(StorageFormats.FileParquet), Some(absPath)) =>
        Some(frameFileStorage.hdfs.size(absPath))
      case (Some(StorageFormats.FileSequence), Some(absPath)) =>
        Some(frameFileStorage.hdfs.size(absPath))
      case _ =>
        warn(s"Could not get size of frame ${frameEntity.id} / ${frameEntity.name}")
        None
    }
  }

  def getReader(frame: FrameEntity)(implicit invocation: Invocation): ParquetReader = {
    withContext("frame.getReader") {
      require(frame != null, "frame is required")
      withMyClassLoader {
        val absPath: Path = new Path(frame.getStorageLocation)
        new ParquetReader(absPath, frameFileStorage.hdfs)
      }
    }
  }

  override def drop(frame: FrameEntity)(implicit invocation: Invocation): Unit = {
    frameFileStorage.delete(frame)
    metaStore.withSession("frame.drop") {
      implicit session =>
        {
          metaStore.frameRepo.delete(frame.id).get
        }
    }
  }

  override def renameFrame(frame: FrameEntity, newName: String)(implicit invocation: Invocation): FrameEntity = {
    metaStore.withSession("frame.rename") {
      implicit session =>
        {
          val check = metaStore.frameRepo.lookupByName(Some(newName))
          if (check.isDefined) {
            throw new RuntimeException("Frame with same name exists. Rename aborted.")
          }
          val newFrame = frame.copy(name = Some(newName))
          val renamedFrame = metaStore.frameRepo.update(newFrame).get
          metaStore.frameRepo.updateLastReadDate(renamedFrame).get
        }
    }
  }

  override def renameColumns(frame: FrameEntity, namePairs: Seq[(String, String)])(implicit invocation: Invocation): FrameEntity =
    metaStore.withSession("frame.renameColumns") {
      implicit session =>
        {
          metaStore.frameRepo.updateSchema(frame, frame.schema.renameColumns(namePairs.toMap))
        }
    }

  override def lookupByName(name: Option[String])(implicit invocation: Invocation): Option[FrameEntity] = {
    metaStore.withSession("frame.lookupByName") {
      implicit session =>
        {
          metaStore.frameRepo.lookupByName(name)
        }
    }
  }

  @deprecated("please use expectFrame() instead")
  override def lookup(id: Long)(implicit invocation: Invocation): Option[FrameEntity] = {
    metaStore.withSession("frame.lookup") {
      implicit session =>
        {
          metaStore.frameRepo.lookup(id)
        }
    }
  }

  override def getFrames()(implicit invocation: Invocation): Seq[FrameEntity] = {
    metaStore.withSession("frame.getFrames") {
      implicit session =>
        {
          metaStore.frameRepo.scanAll().filter(f => f.status != Status.Deleted && f.status != Status.Deleted_Final && f.name.isDefined)
        }
    }
  }

  override def create(arguments: CreateEntityArgs = CreateEntityArgs())(implicit invocation: Invocation): FrameEntity = {
    metaStore.withSession("frame.createFrame") {
      implicit session =>
        {
          if (arguments.name.isDefined) {
            metaStore.frameRepo.lookupByName(arguments.name).foreach {
              existingFrame =>
                throw new DuplicateNameException("frame", arguments.name.get, "Frame with same name exists. Create aborted.")
            }
          }
          val frameTemplate = DataFrameTemplate(arguments.name)
          val frame = metaStore.frameRepo.insert(frameTemplate).get

          //remove any existing artifacts to prevent collisions when a database is reinitialized.
          frameFileStorage.deletePath(frameFileStorage.calculateFramePath(frame))

          frame
        }
    }
  }

  /**
   * Get the error frame of the supplied frame or create one if it doesn't exist
   * @param frame the 'good' frame
   * @return the 'error' frame associated with the 'good' frame
   */
  override def lookupOrCreateErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): FrameEntity = {
    val errorFrame = lookupErrorFrame(frame)
    if (errorFrame.isEmpty) {
      metaStore.withSession("frame.lookupOrCreateErrorFrame") {
        implicit session =>
          {
            val errorTemplate = new DataFrameTemplate(None, Some(s"This frame was automatically created to capture parse errors for ${frame.name} ID: ${frame.id}"))
            val newlyCreatedErrorFrame = metaStore.frameRepo.insert(errorTemplate).get
            metaStore.frameRepo.updateErrorFrameId(frame, Some(newlyCreatedErrorFrame.id))

            //remove any existing artifacts to prevent collisions when a database is reinitialized.
            frameFileStorage.delete(newlyCreatedErrorFrame)

            newlyCreatedErrorFrame
          }
      }
    }
    else {
      errorFrame.get
    }
  }

  /**
   * Get the error frame of the supplied frame
   * @param frame the 'good' frame
   * @return the parse errors for the 'good' frame
   */
  override def lookupErrorFrame(frame: FrameEntity)(implicit invocation: Invocation): Option[FrameEntity] = {
    if (frame.errorFrameId.isDefined) {
      Some(expectFrame(FrameReference(frame.errorFrameId.get)))
    }
    else {
      None
    }
  }

  /**
   * Provides a clean-up context to create and work on a new frame
   *
   * Takes the template, creates a frame and then hands it to a work function.  If any error occurs during the work
   * the frame is deleted from the metastore.  Typical usage would be during the creation of a brand new frame,
   * where data processing needs to occur, any error means the frame should not continue to exist in the metastore.
   *
   * @param args - create entity arguments
   * @param work - Frame to Frame function.  This function typically loads RDDs, does work, and saves RDDS
   * @return - the frame result of work if successful, otherwise an exception is raised
   */
  // TODO: change to return a Try[DataFrame] instead of raising exception?
  def tryNewFrame(args: CreateEntityArgs = CreateEntityArgs())(work: FrameEntity => FrameEntity)(implicit invocation: Invocation): FrameEntity = {
    val frame = create(args)
    Try {
      work(frame)
    } match {
      case Success(f) => f
      case Failure(e) =>
        drop(frame)
        throw e
    }
  }

  /**
   * Set the last read date for a frame to the current time
   * @param frame the frame to update
   */
  def updateLastReadDate(frame: FrameEntity): Option[FrameEntity] = {
    metaStore.withSession("frame.updateLastReadDate") {
      implicit session =>
        {
          if (frame.graphId.isDefined) {
            val graph = metaStore.graphRepo.lookup(frame.graphId.get).get
            metaStore.graphRepo.updateLastReadDate(graph)
          }
          metaStore.frameRepo.updateLastReadDate(frame).toOption
        }
    }
  }

  /**
   * Set a frame to be deleted on the next execution of garbage collection
   * @param frame frame to delete
   * @param invocation current invocation
   */
  override def scheduleDeletion(frame: FrameEntity)(implicit invocation: Invocation): Unit = {
    metaStore.withSession("spark.framestorage.scheduleDeletion") {
      implicit session =>
        {
          info(s"marking as ready to delete: frame id:${frame.id}, name:${frame.name}")
          metaStore.frameRepo.updateReadyToDelete(frame)
        }
    }
  }
}
