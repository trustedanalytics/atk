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

import org.trustedanalytics.atk.domain.frame.DataFrameTemplate
import org.joda.time.DateTime
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.graph.GraphTemplate

class GraphRepositoryTest extends SlickMetaStoreH2Testing with Matchers {

  "GraphRepository" should "be able to create graphs" in {
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("graph-test") {
      implicit session =>

        val name = Some("my_name")

        // create a graph
        val graph = graphRepo.insert(new GraphTemplate(name, "atk/frame"))
        graph.get should not be null

        // look it up and validate expected values
        val graph2 = graphRepo.lookup(graph.get.id)
        graph2.get should not be null
        graph2.get.name shouldBe name
        graph2.get.createdOn should not be null
        graph2.get.modifiedOn should not be null
    }
  }

  it should "return a list of graphs ready to have their data deleted" in {
    val frameRepo = slickMetaStoreComponent.metaStore.frameRepo
    val graphRepo = slickMetaStoreComponent.metaStore.graphRepo
    slickMetaStoreComponent.metaStore.withSession("graph-test") {
      implicit session =>
        val age = 10 * 24 * 60 * 60 * 1000 //10 days

        val name = Some("my_name")

        // create graphs

        //should be in the stale list because it is old and unnamed
        val graph1 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph1.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in stale list because though old it is named
        val graph2 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph2.copy(lastReadDate = new DateTime().minus(age * 2)))

        //should not be in stale list because it is too new
        val graph3 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph3.copy(lastReadDate = new DateTime()))

        //should be in stale list, though it owns a named frame --this is convoluted construction.
        val graph4 = graphRepo.insert(new GraphTemplate(None)).get
        graphRepo.update(graph4.copy(lastReadDate = new DateTime().minus(age * 2)))

        val frame = frameRepo.insert(new DataFrameTemplate(Some("namedFrame"), None)).get
        frameRepo.update(frame.copy(lastReadDate = new DateTime().minus(age * 2), graphId = Some(graph4.id)))

        //should not be in stale list because it has been dropped
        val graph5 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph5.copy(statusId = DroppedStatus))

        //should not be in stale list because it has been finalized
        val graph6 = graphRepo.insert(new GraphTemplate(name)).get
        graphRepo.update(graph6.copy(statusId = FinalizedStatus))

        val stale = graphRepo.getStaleEntities(age).map(g => g.id).toList
        stale should contain(graph1.id)
        stale should contain(graph4.id)
        stale.length should be(2)

        val dropped = graphRepo.droppedGraphs.map(g => g.id).toList
        dropped should contain(graph5.id)
        dropped.length should be(1)
    }
  }
}
