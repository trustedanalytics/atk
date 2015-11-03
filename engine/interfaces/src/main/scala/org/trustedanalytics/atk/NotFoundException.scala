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


package org.trustedanalytics.atk

/**
 * Thrown when a requested resource does not exist.
 */
class NotFoundException(message: String) extends RuntimeException(message) {

  def this(resourceType: String, name: String, additionalMessage: String = "") = {
    this(s"Requested resource of type '$resourceType' named '$name' could not be found.\n $additionalMessage\n")
  }

  def this(resourceType: String, id: Long) = {
    this(s"Requested resource of type '$resourceType' with id '$id' could not be found.\n")
  }

}
