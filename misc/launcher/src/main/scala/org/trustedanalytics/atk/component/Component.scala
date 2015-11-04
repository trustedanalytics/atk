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

/**
 * Base interface for a component / plugin.
 */
trait Component {

  private var configuredName: Option[String] = None
  private var config: Option[Config] = None

  /**
   * A component's name, useful for error messages
   */
  final def componentName: String = {
    configuredName.getOrElse(throw new Exception("This component has not been initialized, so it does not have a name"))
  }

  final def configuration: Config = {
    config.getOrElse(throw new Exception("This component has not been initialized, so it does not have a name"))
  }

  /**
   * Called before processing any requests.
   *
   * @param name          the name that was used to locate this component
   * @param configuration Configuration information, scoped to that required by the
   *                      plugin based on its installed paths.
   */
  final def init(name: String, configuration: Config) = {
    configuredName = Some(name)
    config = Some(configuration)
  }

  /**
   * Called before processing any requests.
   *
   */
  def start() = {}

  /**
   * Called before the application as a whole shuts down. Not guaranteed to be called,
   * nor guaranteed that the application will not shut down while this method is running,
   * though an effort will be made.
   */
  def stop() = {}
}
