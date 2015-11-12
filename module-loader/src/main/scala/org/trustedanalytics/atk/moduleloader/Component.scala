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

/**
 * Interface that can be implemented in modules.
 *
 * Implementation can be an entry point for applications started by the ModuleLoader
 */
trait Component {

  /**
   * Start this component
   */
  def start(): Unit

  /**
   * Stop this component
   */
  def stop(): Unit

}
