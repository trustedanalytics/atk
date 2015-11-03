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


package org.trustedanalytics.atk.component

import com.typesafe.config.Config

import scala.reflect.ClassTag

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class DefaultArchive(archiveDefinition: ArchiveDefinition, classLoader: ClassLoader, config: Config)
    extends Archive(archiveDefinition, classLoader, config) {

  /**
   * Obtain instances of a given class. The keys are established purely
   * by convention.
   *
   * @param descriptor the string key of the desired class instance.
   * @tparam T the type of the requested instances
   * @return the requested instances, or the empty sequence if no such instances could be produced.
   */
  override def getAll[T: ClassTag](descriptor: String): Seq[T] = {
    val array = try {
      Archive.logger(s"Archive ${definition.name} getting all '$descriptor'")
      val stringList = configuration.getStringList(descriptor + ".available").asScala
      Archive.logger(s"Found: $stringList")
      val components = stringList.map(componentName => loadComponent(descriptor + "." + componentName))
      components.map(_.asInstanceOf[T]).toArray
    }
    catch {
      case NonFatal(e) =>
        Archive.logger(e.toString)
        Archive.logger(configuration.root().render())
        throw e
    }
    array
  }

}
