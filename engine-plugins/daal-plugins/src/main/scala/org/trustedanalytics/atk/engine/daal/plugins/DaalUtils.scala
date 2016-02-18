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

package org.trustedanalytics.atk.engine.daal.plugins

import org.apache.commons.lang.StringUtils

object DaalUtils extends Serializable {

  val DaalRequiredLibraries = List("libAtkDaalJavaAPI.so", "libiomp5.so", "libJavaAPI.so", "/libtbb.so.2")

  /**
   * Check that all required libraries are specified in configuration.
   *
   * Throws an exception if any of the required libraries are not specified in configuration
   *
   * @param confLibraryPath String with comma-separated list of DAAL libraries in configuration
   */
  def validateDaalLibraries(confLibraryPath: String): Unit = {
    require(StringUtils.isNotEmpty(confLibraryPath), "trustedanalytics.atk.engine.spark.daal.dynamic-libraries cannot be empty. " + "" +
      "Please set path to DAAL libraries in configuration file.")
    val libraryFiles = confLibraryPath.split(",")

    DaalRequiredLibraries.foreach(library => {
      val pattern = s".*(${library})".r
      if (!libraryFiles.exists(f => pattern.findFirstIn(f).isDefined)) {
        throw new scala.IllegalArgumentException(s"Please add path to ${library} in trustedanalytics.atk.engine.spark.daal.dynamic-libraries.")
      }
    })
  }
}
