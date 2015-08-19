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

import org.trustedanalytics.atk.component.{ ArchiveDefinition, ClassLoaderAware, Archive }
import com.typesafe.config.Config

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import org.trustedanalytics.atk.event.EventLogging
import _root_.kamon.Kamon

class EngineApplication(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) with EventLogging with ClassLoaderAware {
  if (EventLogging.raw) {
    info("Engine setting log adapter from configuration")
    EventLogging.raw = configuration.getBoolean("trustedanalytics.atk.engine.logging.raw")
    info("Engine set log adapter from configuration")
  } // else rest-server already installed an SLF4j adapter

  EventLogging.profiling = configuration.getBoolean("trustedanalytics.atk.engine.logging.profile")
  info(s"Engine profiling: ${EventLogging.profiling}")

  var engine: EngineComponent = null

  override def getAll[T: ClassTag](descriptor: String) = {
    descriptor match {
      case "engine" => Seq(engine.engine.asInstanceOf[T])
      case _ => Seq()
    }
  }

  override def stop() = {
    info("Shutting down engine")
    Kamon.shutdown()
    engine.engine.shutdown()
  }

  override def start() = {
    try {
      Kamon.start()
      engine = new EngineComponent
    }
    catch {
      case NonFatal(e) =>
        error("An error occurred while starting the engine.", exception = e)
        throw e
    }
  }
}

