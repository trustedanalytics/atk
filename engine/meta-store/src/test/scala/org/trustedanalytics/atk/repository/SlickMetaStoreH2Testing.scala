/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.event.EventContext
import org.scalatest.{ BeforeAndAfter, FlatSpec }

import scala.slick.driver.H2Driver
import scala.util.Random

/**
 * Trait can be mixed into ScalaTests to provide a meta store backed by H2.
 *
 * Creates and drops all tables before and after each test.
 */
trait SlickMetaStoreH2Testing extends FlatSpec with BeforeAndAfter {

  val ActiveStatus: Long = 1
  val DeletedStatus: Long = 2
  val DeletedFinalStatus: Long = 3

  implicit val rootEc = EventContext.enter("root")

  lazy val slickMetaStoreComponent: SlickMetaStoreComponent = new SlickMetaStoreComponent with DbProfileComponent {
    override lazy val profile = new Profile(H2Driver, connectionString = "jdbc:h2:mem:atktest" + Random.nextInt() + ";DB_CLOSE_DELAY=-1", driver = "org.h2.Driver")
  }

  before {
    // initialize tables
    slickMetaStoreComponent.metaStore.initializeSchema()
  }

  after {
    // drop all tables
    slickMetaStoreComponent.metaStore.dropAllTables()
  }

}
