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

package org.trustedanalytics.atk.engine.gc

import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.util.concurrent.{ Executors, TimeUnit, ScheduledFuture }

import org.trustedanalytics.atk.domain.graph.GraphEntity
import org.trustedanalytics.atk.domain.model.ModelEntity
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.gc.{ GarbageCollectionEntryTemplate, GarbageCollectionEntry, GarbageCollectionTemplate, GarbageCollection }
import org.trustedanalytics.atk.engine.{ EngineExecutionContext, GraphBackendStorage, EngineConfig }
import org.trustedanalytics.atk.engine.plugin.BackendInvocation
import org.trustedanalytics.atk.engine.frame.FrameFileStorage
import org.trustedanalytics.atk.repository.MetaStore
import org.joda.time.DateTime

/**
 * Runnable Thread that executes garbage collection of unused entities.
 * @param metaStore database store
 * @param frameFileStorage storage class for accessing frame file storage
 * @param graphBackendStorage storage class for accessing graph backend storage
 */
class GarbageCollector(val metaStore: MetaStore, val frameFileStorage: FrameFileStorage, val graphBackendStorage: GraphBackendStorage) extends Runnable with EventLogging {

  val start = new DateTime()
  val gcRepo = metaStore.gcRepo
  val gcEntryRepo = metaStore.gcEntryRepo
  val frames = metaStore.frameRepo
  val graphs = metaStore.graphRepo
  val models = metaStore.modelRepo

  /**
   * @return get host name of computer executing this process
   */
  def hostname: String =
    InetAddress.getLocalHost.getHostName

  /**
   * @return get the process id of the executing process
   */
  def processId: Long =
    ManagementFactory.getRuntimeMXBean.getName.split("@")(0).toLong

  /**
   * Execute Garbage Collection as a Runnable
   */
  override def run(): Unit = {
    runAllPhases()
  }

  /**
   * Identify and drop all stale entities
   * @param gcStaleAge - age at which an entity become stale, from last access (in ms)
   */
  def dropStale(gcStaleAge: Long = EngineConfig.gcStaleAge): Unit = {
    metaStore.withSession("gc.garbagecollector.dropStale") {
      implicit session =>
        try {
          info("Execute Garbage Collector Finalize")
          frames.getStaleEntities(gcStaleAge).foreach(frame => frames.dropFrame(frame))
          graphs.getStaleEntities(gcStaleAge).foreach(graph => graphs.dropGraph(graph))
          models.getStaleEntities(gcStaleAge).foreach(model => models.dropModel(model))
        }
        catch {
          case e: Exception => error("Exception Thrown during Garbage Collector DropStale", exception = e)
        }
    }
  }

  /**
   * Finalize all dropped Entities
   */
  def finalizeEntities(gc: GarbageCollection): Unit = {
    metaStore.withSession("gc.garbagecollector.finalizeEntities") {
      implicit session =>
        try {
          info("Execute Garbage Collector Finalize")
          finalizeFrames(gc)
          finalizeGraphs(gc)
          finalizeModels(gc)
        }
        catch {
          case e: Exception => error("Exception Thrown during Garbage Collector Finalize", exception = e)
        }
    }
  }

  def runAllPhases(gcStaleAge: Long = EngineConfig.gcStaleAge): Unit = {
    this.synchronized {
      metaStore.withSession("gc.garbagecollector") {
        implicit session =>
          try {
            val gc: GarbageCollection = gcRepo.insert(new GarbageCollectionTemplate(hostname, processId, new DateTime)).get
            dropStale(gcStaleAge)
            finalizeEntities(gc)
            gcRepo.updateEndTime(gc)
          }
          catch {
            case e: Exception => error("Exception Thrown during Garbage Collection", exception = e)
          }
      }
    }
  }

  /**
   * finalize all frames that have been dropped
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def finalizeFrames(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    metaStore.frameRepo.droppedFrames.foreach(frame => { finalizeFrame(gc, frame) })
  }

  /**
   * Deletes data associated with a frame and places an entry into the GarbageCollectionEntry table
   * @param gc garbage collection database entry
   * @param frame frame to be deleted
   */
  def finalizeFrame(gc: GarbageCollection, frame: FrameEntity)(implicit session: metaStore.Session): Unit = {
    val description = s"finalize frame id=${frame.id} name=${frame.name}"
    try {
      val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
        new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
      info(description)
      frameFileStorage.deleteFrameData(frame)
      metaStore.frameRepo.finalizeEntity(frame)
      gcEntryRepo.updateEndTime(gcEntry)
    }
    catch {
      case e: Exception => error(s"Exception trying to $description", exception = e)
    }
  }

  /**
   * Finalize all graphs that have been dropped.
   * garbage collect graphs delete underlying frame rdds for a seamless graph and mark as deleted
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def finalizeGraphs(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    metaStore.graphRepo.droppedGraphs.foreach(graph => { finalizeGraph(gc, graph) })
  }

  /**
   * Deletes data associated with a graph and places an entry into the GarbageCollectionEntry table
   * @param gc garbage collection database entry
   * @param graph graph to be deleted
   */
  def finalizeGraph(gc: GarbageCollection, graph: GraphEntity)(implicit session: metaStore.Session): Unit = {
    val description = s"finalize graph id=${graph.id} name=${graph.name}"
    try {
      val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
        new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
      info(description)
      metaStore.frameRepo.lookupByGraphId(graph.id).foreach(frame => finalizeFrame(gc, frame))
      metaStore.graphRepo.finalizeEntity(graph)
      if (graph.isTitan) {
        graphBackendStorage.deleteUnderlyingTable(graph.storage, quiet = true)(invocation = new BackendInvocation(EngineExecutionContext.global))
      }
      gcEntryRepo.updateEndTime(gcEntry)
    }
    catch {
      case e: Exception => error(s"Exception trying to $description", exception = e)
    }
  }

  /**
   * finalize all models that have been dropped
   * @param gc garbage collection database entry
   * @param session db session for backend process
   */
  def finalizeModels(gc: GarbageCollection)(implicit session: metaStore.Session): Unit = {
    metaStore.modelRepo.droppedModels.foreach(model => { finalizeModel(gc, model) })
  }

  /**
   * Deletes data associated with a model and places an entry into the GarbageCollectionEntry table
   * @param gc garbage collection database entry
   * @param model model to be deleted
   */
  def finalizeModel(gc: GarbageCollection, model: ModelEntity)(implicit session: metaStore.Session): Unit = {
    val description = s"finalize model id=${model.id} name=${model.name}"
    try {
      val gcEntry: GarbageCollectionEntry = gcEntryRepo.insert(
        new GarbageCollectionEntryTemplate(gc.id, description, new DateTime)).get
      info(description)
      metaStore.modelRepo.finalizeEntity(model)
      gcEntryRepo.updateEndTime(gcEntry)
    }
    catch {
      case e: Exception => error(s"Exception trying to $description", exception = e)
    }
  }
}

object GarbageCollector {
  private[this] var gcScheduler: ScheduledFuture[_] = null
  private[this] var garbageCollector: GarbageCollector = null

  /**
   * start the garbage collector thread
   * @param metaStore database store
   * @param frameStorage storage class for accessing frame storage
   * @param graphBackendStorage storage class for accessing graph backend storage
   */
  def startup(metaStore: MetaStore, frameStorage: FrameFileStorage, graphBackendStorage: GraphBackendStorage): Unit = {
    this.synchronized {
      if (garbageCollector == null)
        garbageCollector = new GarbageCollector(metaStore, frameStorage, graphBackendStorage)
      if (gcScheduler == null) {
        gcScheduler = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(garbageCollector, 0, EngineConfig.gcInterval, TimeUnit.MILLISECONDS)
      }
    }
  }

  /**
   * Execute a garbage collection outside of the regularly scheduled intervals
   * @param gcStaleAge in milliseconds
   */
  def singleTimeExecution(gcStaleAge: Option[Long] = None): Unit = {
    require(garbageCollector != null, "GarbageCollector has not been initialized. Problem during RestServer initialization")
    gcStaleAge match {
      case Some(age) => garbageCollector.runAllPhases(age)
      case None => garbageCollector.runAllPhases()
    }
  }

  /**
   * shutdown the garbage collector thread
   */
  def shutdown(): Unit = {
    this.synchronized {
      if (gcScheduler != null)
        gcScheduler.cancel(false)
    }
  }
}
