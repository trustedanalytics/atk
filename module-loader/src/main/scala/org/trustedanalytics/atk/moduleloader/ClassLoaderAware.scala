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

package org.trustedanalytics.atk.moduleloader

trait ClassLoaderAware {

  /**
   * Execute a code block setting the ContextClassLoader to the ClassLoader of 'this' class.
   *
   * Sometimes 3rd party libraries will use the ContextClassLoader
   */
  def withMyClassLoader[T](expr: => T): T = {
    withContextClassLoader(this.getClass.getClassLoader)(expr)
  }

  /**
   * Execute a code block using specified class loader as the ContextClassLoader
   *
   * Sometimes 3rd party libraries will use the ContextClassLoader
   */
  def withContextClassLoader[T](loader: ClassLoader)(expr: => T): T = {
    val prior = Thread.currentThread().getContextClassLoader
    try {
      Thread.currentThread().setContextClassLoader(loader)
      expr
    }
    finally {
      Thread.currentThread().setContextClassLoader(prior)
    }
  }
}
