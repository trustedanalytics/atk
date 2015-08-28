package org.trustedanalytics.atk.engine.metrics

import java.util.concurrent.{ Executors, ScheduledFuture, TimeUnit }

import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.event.EventLogging
import kamon.Kamon
import org.trustedanalytics.atk.repository.MetaStore

/**
 * Runnable Thread that executes slick queries that are reported outside the rest-server through kamon api.
 * Currently reports only to statsd. Although all kamon reporting api's are usable only statsd is packaged with engine-core.
 * Kamon statsd documentation can be found at http://kamon.io/backends/statsd/
 * @param metaStore database store
 */
class MetricCollection(val metaStore: MetaStore) extends Runnable with EventLogging {

  /*
  Define metrics that will be reported out
   */
  val framesTotal = Kamon.metrics.histogram("frames-total")
  val framesSuccessful = Kamon.metrics.histogram("frames-successful")
  val framesError = Kamon.metrics.histogram("frames-error")

  val commandsTotal = Kamon.metrics.histogram("commands-total")
  val commandsSuccessful = Kamon.metrics.histogram("commands-successful")
  val commandsError = Kamon.metrics.histogram("commands-error")

  val graphHistogram = Kamon.metrics.histogram("graphs")

  override def run(): Unit = {
    collect()
  }

  /**
   * By default this will only run every 60s if metric collection is turned on.
   */
  def collect() {
    this.synchronized {
      metaStore.withSession("metric.collect") {
        implicit session =>
          {

            info("metric collection")
            framesTotal.record(metaStore.frameRepo.totalCount())
            framesSuccessful.record(metaStore.frameRepo.successfulCount())
            framesError.record(metaStore.frameRepo.errorCount())

            commandsTotal.record(metaStore.commandRepo.totalCount())
            commandsSuccessful.record(metaStore.commandRepo.successfulCount())
            commandsError.record(metaStore.commandRepo.errorCount())

            graphHistogram.record(metaStore.graphRepo.totalCount())

          }
      }
    }
  }

}

object MetricCollection {
  private[this] var kcScheduler: ScheduledFuture[_] = null
  private[this] var kamonCollect: MetricCollection = null

  def startup(metaStore: MetaStore): Unit = {
    if (EngineConfig.metricStart) {
      this.synchronized {
        if (kamonCollect == null)
          kamonCollect = new MetricCollection(metaStore)
        if (kcScheduler == null) {
          kcScheduler = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(kamonCollect, 0, EngineConfig.metricTick, TimeUnit.SECONDS)
        }
      }
    }
  }

  /**
   * Execute metrics collection outside of the regularly scheduled intervals
   */
  def singleTimeExecution(): Unit = {
    require(kamonCollect != null, "metric collector has not been initialized. Problem during RestServer initialization")
    kamonCollect.collect()
  }

  /**
   * shutdown the metric collector thread
   */
  def shutdown(): Unit = {
    this.synchronized {
      if (kcScheduler != null)
        kcScheduler.cancel(false)
    }
  }
}
