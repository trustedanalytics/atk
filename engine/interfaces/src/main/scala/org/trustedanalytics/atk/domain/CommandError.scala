/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.domain

import org.apache.commons.lang3.exception.ExceptionUtils

/**
 * Holds an error from running a command plugin
 * @param message displayed to user
 * @param stackTrace for diagnostics
 */
case class CommandError(message: String, stackTrace: Option[String])

object CommandError {

  def appendError(currentError: Option[CommandError], additionalException: Throwable): CommandError = {
    currentError match {
      case Some(error) =>
        val appendedStackTrace = error.stackTrace.getOrElse("") + "\n" + ExceptionUtils.getStackTrace(additionalException)
        error.copy(stackTrace = Some(appendedStackTrace))
      case None => throwableToError(additionalException)
    }
  }

  private def throwableToError(t: Throwable): CommandError = {
    val message = t.getMessage match {
      case null | "" => t.getClass.getName
      case s => s
    }
    CommandError(message, stackTrace = Some(ExceptionUtils.getStackTrace(t)))
  }

}