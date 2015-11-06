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

package org.trustedanalytics.atk.repository

import org.scalatest.{ FlatSpec, Matchers }

import scala.slick.driver.{ H2Driver, PostgresDriver }

class ProfileTest extends FlatSpec with Matchers {

  "Profile" should "be able to determine the JdbcProfile from the driver name org.h2.Driver" in {
    assert(Profile.jdbcProfileForDriver("org.h2.Driver").isInstanceOf[H2Driver])
  }

  it should "be able to determine the JdbcProfile from the driver name org.postgresql.Driver" in {
    assert(Profile.jdbcProfileForDriver("org.postgresql.Driver").isInstanceOf[PostgresDriver])
  }

  it should "throw an exception for unsupported drivers" in {
    try {
      Profile.jdbcProfileForDriver("com.example.Driver")

      fail("The expected Exception was NOT thrown")
    }
    catch {
      case e: IllegalArgumentException => // Expected
    }
  }
}
