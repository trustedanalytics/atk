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

import java.io.File

import com.intel.daal.services.DaalContext
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

  /**
   * Get list of files to DAAL libraries
   *
   * @param confLibraryPath  String with comma-separated list of DAAL libraries in configuration
   * @return List of files to DAAL libraries
   */
  def getDaalLibraryPaths(confLibraryPath: String): List[File] = {
    validateDaalLibraries(confLibraryPath)
    val paths = confLibraryPath.split(",").toList
    paths.map(p => new File(p))
  }

  /**
   * Interface for DAAL context results
   * @tparam T Return type for operation on DAAL context
   */
  trait DaalContextResult[T] {

    /**
     * Return result of type T or throw exception with provided error message
     * @param message Error message to display in case of exception
     * @return Result of type T or exception in case of error
     */
    def elseError(message: String): T
  }

  /**
   * Return type for successful operations with DAAL context
   * @param result Successful result
   */
  class DaalContextSuccessResult[T](result: T) extends DaalContextResult[T] {

    /**
     * Return valid result on successful operation
     * @param message Error message to display in case of exception
     * @return Valid result
     */
    override def elseError(message: String): T = result
  }

  /**
   * Return type for failed operations with DAAL context
   * @param ex Exception thrown
   */
  class DaalContextErrorResult[T](ex: Exception) extends DaalContextResult[T] {

    /**
     * Throw exception on failed operation
     * @param message Error message to display
     * @return Exception thrown
     */
    override def elseError(message: String): T = {
      throw new RuntimeException(message, ex)
    }
  }

  /**
   * Execute a code block using DAAL context
   *
   * @param func Function which uses DAAL context
   * @return Valid result of type T, or exception in case of error
   */
  def withDaalContext[T](func: DaalContext => T): DaalContextResult[T] = {
    val context = new DaalContext()
    try {
      new DaalContextSuccessResult[T](func(context))
    }
    catch {
      case ex: Exception => new DaalContextErrorResult[T](ex)
    }
    finally {
      context.dispose()
    }
  }

}
