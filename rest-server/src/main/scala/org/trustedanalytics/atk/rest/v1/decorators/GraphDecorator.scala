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
package org.trustedanalytics.atk.rest.v1.decorators

import org.trustedanalytics.atk.domain.Status
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.graph.{ SeamlessGraphMeta, GraphEntity }
import org.trustedanalytics.atk.rest.v1.viewmodels.{ RelLink, GetGraph, GetGraphs }

/**
 * Decoration object for graph components, whether vertex or edge
 * @param label - graph component label --i.e. the type of vertex or edge
 * @param count - number of items of this component --e.g. a VertexFrame.rowCount
 * @param properties - the names of the properties associated with this component
 */
case class GetGraphComponent(label: String, count: Long, properties: List[String])

/**
 * Class used to decorate a graph, augments the GraphEntity for decoration without destroying the Decorate API
 * @param graph - graph entity
 * @param vertexFrames - its vertex frames' labels and uris
 * @param edgeFrames - its edge frames' labels and uris
 */
case class DecorateReadyGraphEntity(graph: GraphEntity, vertexFrames: List[GetGraphComponent], edgeFrames: List[GetGraphComponent])

/**
 * A decorator that takes an entity from the database and converts it to a View/Model
 * for delivering via REST services
 */
object GraphDecorator extends EntityDecorator[DecorateReadyGraphEntity, GetGraphs, GetGraph] {

  /**
   * Decorate a single entity (like you would want in "GET /entities/id")
   *
   * @param uri UNUSED? DELETE?
   * @param links related links
   * @param entity the entity to decorate
   * @return the View/Model
   */
  override def decorateEntity(uri: String, links: Iterable[RelLink], entity: DecorateReadyGraphEntity): GetGraph = {

    GetGraph(uri = entity.graph.uri,
      name = entity.graph.name,
      links = links.toList,
      entity.graph.entityType,
      entity.vertexFrames,
      entity.edgeFrames,
      (entity.graph.statusId: Status).name,
      entity.graph.lastReadDate)
  }

  /**
   * Decorate a list of entities (like you would want in "GET /entities")
   *
   * @param uri the base URI, for this type of entity "../entities"
   * @param entities the list of entities to decorate
   * @return the View/Model
   */
  override def decorateForIndex(uri: String, entities: Seq[DecorateReadyGraphEntity]): List[GetGraphs] = {
    entities.map(g => new GetGraphs(id = g.graph.id,
      name = g.graph.name,
      url = uri + "/" + g.graph.id,
      entityType = g.graph.entityType)).toList
  }

}
