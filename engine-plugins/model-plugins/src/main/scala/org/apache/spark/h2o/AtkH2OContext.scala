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

import org.apache.spark.SparkContext

object AtkH2OContext {
  /**
   * Get existing or create new H2OContext based on provided H2O configuration
   *
   * @param sc Spark Context
   * @return H2O Context
   */
  def create(sc: SparkContext): H2OContext = synchronized {
    val constructor = classOf[H2OContext].getConstructor(classOf[SparkContext], classOf[H2OConf])
    constructor.setAccessible(true)
    val context = constructor.newInstance(sc, new H2OConf(sc))
    context.init()

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
