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


package org.trustedanalytics.atk.testutils

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

/**
 * Utility methods for working with directories.
 */
object DirectoryUtils {

  private val log: Logger = Logger.getLogger(DirectoryUtils.getClass)

  /**
   * Create a Temporary directory
   * @param prefix the prefix for the directory name, this is used to make the Temp directory more identifiable.
   * @return the temporary directory
   */
  def createTempDirectory(prefix: String): File = {
    try {
      val tmpDir = convertFileToDirectory(File.createTempFile(prefix, "-tmp"))

      // Don't rely on this- it is just an extra safety net
      tmpDir.deleteOnExit()

      tmpDir
    }
    catch {
      case e: Exception =>
        throw new RuntimeException("Could NOT initialize temp directory, prefix: " + prefix, e)
    }
  }

  /**
   * Convert a file into a directory
   * @param file a file that isn't a directory
   * @return directory with same name as File
   */
  private def convertFileToDirectory(file: File): File = {
    file.delete()
    if (!file.mkdirs()) {
      throw new RuntimeException("Failed to create tmpDir: " + file.getAbsolutePath)
    }
    file
  }

  def deleteTempDirectory(tmpDir: File) {
    FileUtils.deleteQuietly(tmpDir)
    if (tmpDir != null && tmpDir.exists) {
      log.error("Failed to delete tmpDir: " + tmpDir.getAbsolutePath)
    }
  }
}
