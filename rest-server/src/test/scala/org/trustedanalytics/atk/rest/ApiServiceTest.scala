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

import org.trustedanalytics.atk.rest.v1.ApiV1Service

class ApiServiceTest extends ServiceTest {

  val apiV1Service = mock[ApiV1Service]
  val authenticationDirective = mock[AuthenticationDirective]
  val commonDirectives = new CommonDirectives(authenticationDirective)
  val apiService = new ApiService(commonDirectives, apiV1Service)

  "ApiService" should "return a greeting for GET requests to the root path" in {
    Get() ~> apiService.serviceRoute ~> check {
      assert(responseAs[String].contains("Welcome to the Trusted Analytics Toolkit API Server"))
      assert(status.intValue == 200)
      assert(status.isSuccess)
    }
  }

  it should "provide version info as JSON" in {
    Get("/info") ~> apiService.serviceRoute ~> check {
      println(responseAs[String])
      assert(responseAs[String] == """{
                                     |  "name": "Trusted Analytics",
                                     |  "identifier": "ia",
                                     |  "api_versions": ["v1"]
                                     |}""".stripMargin)
      assert(status.intValue == 200)
      assert(status.isSuccess)
    }
  }

}
