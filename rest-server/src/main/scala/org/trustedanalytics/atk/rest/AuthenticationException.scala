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

package org.trustedanalytics.atk.rest

/**
 * Exception class to be thrown in case of Invalid Authentication. CommonDirectives class checks for this exception
 * to respond back to client with a 401 Error response code (StatusCode.Unauthorized)
 * @param message Authentication Exception message
 * @param cause Cause of Authentication Exception
 */
class AuthenticationException(message: String = null, cause: Throwable = null) extends Exception(message, cause)
