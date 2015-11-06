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

import java.net.{ URLClassLoader, URL }

/**
 * Packages a class loader with some additional error handling / logging information
 * that's useful for Archives.
 *
 * Loads classes first from its parent, then from the URLs provided, in order
 */
class ArchiveClassLoader(archiveName: String, urls: Array[URL], parentClassLoader: ClassLoader)
    extends URLClassLoader(urls, parentClassLoader) {

  override def loadClass(className: String, resolve: Boolean): Class[_] = {
    //Interestingly, cannot use "attempt" here, have to use try/catch, probably due to stack depth check in ClassLoader.
    try {
      super.loadClass(className, resolve)
    }
    catch {
      case e: ClassNotFoundException =>
        throw new ClassNotFoundException(s"Could not find class $className in archive $archiveName", e)
    }
  }

  override def toString = {
    s"ArchiveClassLoader($archiveName, [${urls.mkString(", ")}], $parentClassLoader)"
  }
}
