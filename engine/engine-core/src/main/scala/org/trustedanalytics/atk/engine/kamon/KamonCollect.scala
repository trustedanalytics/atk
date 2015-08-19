package org.trustedanalytics.atk.engine.kamon

import java.util.concurrent.{ Executors, ScheduledFuture, TimeUnit }

import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.event.EventLogging
import kamon.Kamon
import org.trustedanalytics.atk.repository.MetaStore

class KamonCollect(val metaStore: MetaStore) extends Runnable with EventLogging {

  val framesTotal = Kamon.metrics.histogram("frames-total")
  val framesSuccessful = Kamon.metrics.histogram("frames-successful")
  val framesError = Kamon.metrics.histogram("frames-error")
  val graphHistogram = Kamon.metrics.histogram("graphs")
  val commandsTotal = Kamon.metrics.histogram("commands-total")
  val commandsSuccessful = Kamon.metrics.histogram("commands-successful")
  val commandsError = Kamon.metrics.histogram("commands-error")

  override def run(): Unit = {
    collect()
  }

  def collect() {
    this.synchronized {
      metaStore.withSession("kamon.collect") {
        implicit session =>
          {
            info("kamon collect")
            framesTotal.record(metaStore.frameRepo.scanAll().length)
            framesSuccessful.record(metaStore.frameRepo.scanSuccessful().length)
            framesError.record(metaStore.frameRepo.scanError().length)

            commandsTotal.record(metaStore.commandRepo.scan().length)
            commandsSuccessful.record(metaStore.commandRepo.scanSuccessful())
            commandsError.record(metaStore.commandRepo.scanError())

            graphHistogram.record(metaStore.graphRepo.scanAll().length)
          }
      }
    }
  }

}

object KamonCollect {
  private[this] var kcScheduler: ScheduledFuture[_] = null
  private[this] var kamonCollect: KamonCollect = null

  def startup(metaStore: MetaStore): Unit = {
    if (EngineConfig.kamonStart) {
      this.synchronized {
        if (kamonCollect == null)
          kamonCollect = new KamonCollect(metaStore)
        if (kcScheduler == null) {
          kcScheduler = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(kamonCollect, 0, EngineConfig.kamonTick, TimeUnit.SECONDS)
        }
      }
    }
  }

  def singleTimeExecution(): Unit = {
    require(kamonCollect != null, "Kamon collector has not been initialized. Problem during RestServer initialization")
    kamonCollect.collect()
  }

  def shutdown(): Unit = {
    this.synchronized {
      if (kcScheduler != null)
        kcScheduler.cancel(false)
    }
  }
}
