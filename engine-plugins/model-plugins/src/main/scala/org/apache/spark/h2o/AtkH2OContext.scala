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
package org.apache.spark.h2o

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.h2o.backends.{ SparklingBackend, SharedH2OConf }
import org.apache.spark.h2o.backends.internal.{ AtkInternalH2OBackend, InternalBackendUtils, SpreadRDDBuilder, InternalBackendConf }
import org.apache.spark.listeners.ExecutorAddNotSupportedListener
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkEnv, SparkContext }
import org.apache.spark.h2o.utils.NodeDesc
import water.api.RestAPIManager
import water.{ H2O, H2OStarter }

import scala.collection.mutable

object AtkH2OContext extends org.apache.spark.Logging with Serializable {

  /**
   * Get existing or create new H2OContext based on provided H2O configuration
   *
   * @param sc Spark Context
   * @return H2O Context
   */
  def resetSparkContext(h2oContext: H2OContext, sc: SparkContext): H2OContext = synchronized {
    val sparkContextField = classOf[H2OContext].getDeclaredField("sparkContext")
    val confField = classOf[H2OContext].getDeclaredField("_conf")
    val sqlcField = classOf[H2OContext].getDeclaredField("sqlc")
    sparkContextField.setAccessible(true)
    confField.setAccessible(true)
    sqlcField.setAccessible(true)

    val conf = new H2OConf(sc)
    val sqlc = SQLContext.getOrCreate(sc)
    sparkContextField.set(h2oContext, sc)
    confField.set(h2oContext, conf)
    sqlcField.set(h2oContext, sqlc)
    h2oContext
  }
  /**
   * Get existing or create new H2OContext based on provided H2O configuration
   *
   * @param sc Spark Context
   * @return H2O Context
   */
  def forceInit(sc: SparkContext): H2OContext = synchronized {
    val h2oContextClass = classOf[H2OContext]

    val constructor = h2oContextClass.getConstructor(classOf[SparkContext], classOf[H2OConf])
    val h2oNodeField = h2oContextClass.getDeclaredField("h2oNodes")
    val localClientIpField = h2oContextClass.getDeclaredField("localClientIp")
    val localClientPortField = h2oContextClass.getDeclaredField("localClientPort")

    constructor.setAccessible(true)
    h2oNodeField.setAccessible(true)
    localClientIpField.setAccessible(true)
    localClientPortField.setAccessible(true)

    val h2OContext = constructor.newInstance(sc, new H2OConf(sc))
    val h2oNodes = h2oNodeField.get(h2OContext).asInstanceOf[mutable.ArrayBuffer[NodeDesc]]
    if (!h2OContext.isRunningOnCorrectSpark(sc)) {
      throw new WrongSparkVersion(s"You are trying to use Sparkling Water built for Spark ${h2OContext.buildSparkMajorVersion}," +
        s" but your $$SPARK_HOME(=${sc.getSparkHome().getOrElse("SPARK_HOME is not defined!")}) property" +
        s" points to Spark of version ${sc.version}. Please ensure correct Spark is provided and" +
        s" re-run Sparkling Water.")
    }

    // Init the H2O Context in a way provided by used backend and return the list of H2O nodes in case of external
    // backend or list of spark executors on which H2O runs in case of internal backend
    val backend: SparklingBackend = new AtkInternalH2OBackend(h2OContext)
    val nodes = backend.init()

    // Fill information about H2O client and H2O nodes in the cluster
    h2oNodes.append(nodes: _*)
    h2oNodeField.set(h2OContext, h2oNodes)

    localClientIpField.set(h2OContext, H2O.SELF_ADDRESS.getHostAddress)
    localClientPortField.set(h2OContext, H2O.API_PORT)
    logInfo("Sparkling Water started, status of context: " + this.toString)

    h2OContext
  }

  /*
  private[H2OContext] def setInstantiatedContext(h2oContext: H2OContext): Unit = {
    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null) {
        instantiatedContext.set(h2oContext)
      }
    }
  }

  @transient private val instantiatedContext = new AtomicReference[H2OContext]()

  /**
   * Tries to get existing H2O Context. If it is not there, ok.
   * Note that this method has to be here because otherwise ScalaCodeHandlerSuite will fail in one of the tests.
   * If you want to throw an exception when the context is missing, use ensure()
   * If you want to create the context if it is not missing, use getOrCreate() (if you can).
   *
   * @return Option containing H2O Context or None
   */
  def get(): Option[H2OContext] = Option(instantiatedContext.get())

  def ensure(onError: => String = "H2OContext has to be started in order to save/load data using H2O Data source."): H2OContext =
    Option(instantiatedContext.get()) getOrElse {
      throw new RuntimeException(onError)
    }

  /**
   * Get existing or create new H2OContext based on provided H2O configuration
   *
   * @param sc Spark Context
   * @param conf H2O configuration
   * @return H2O Context
   */
  def getOrCreate(sc: SparkContext, conf: H2OConf): H2OContext = synchronized {
    if (instantiatedContext.get() == null) {
      instantiatedContext.set(new H2OContext(sc, conf))
      instantiatedContext.get().init()
    }
    instantiatedContext.get()
  }

  /**
   * Get existing or create new H2OContext based on provided H2O configuration. It searches the configuration
   * properties passed to Sparkling Water and based on them starts H2O Context. If the values are not found, the default
   * values are used in most of the cases. The default cluster mode is internal, ie. spark.ext.h2o.external.cluster.mode=false
   *
   * @param sc Spark Context
   * @return H2O Context
   */
  def getOrCreate(sc: SparkContext): H2OContext = {
    getOrCreate(sc, new H2OConf(sc))
  }   */
}
