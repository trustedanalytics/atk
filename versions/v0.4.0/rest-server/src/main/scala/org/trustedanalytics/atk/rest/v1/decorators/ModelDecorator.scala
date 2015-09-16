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

package org.trustedanalytics.atk.rest.v1.decorators

import org.trustedanalytics.atk.domain.Status
import org.trustedanalytics.atk.domain.model.ModelEntity
import org.trustedanalytics.atk.rest.v1.viewmodels.{ RelLink, GetModels, GetModel }

/**
 * A decorator that takes an entity from the database and converts it to a View/Model
 * for delivering via REST services
 */
object ModelDecorator extends EntityDecorator[ModelEntity, GetModels, GetModel] {

  /**
   * Decorate a single entity (like you would want in "GET /entities/id")
   *
   * @param uri the uri to the current entity
   * @param links related links
   * @param entity the entity to decorate
   * @return the View/Model
   */
  override def decorateEntity(uri: String, links: Iterable[RelLink], entity: ModelEntity): GetModel = {

    GetModel(uri = entity.uri, name = entity.name, links.toList, entity.entityType,
      Status.getName(entity.statusId))
  }

  /**
   * Decorate a list of entities (like you would want in "GET /entities")
   *
   * @param uri the base URI, for this type of entity "../entities"
   * @param entities the list of entities to decorate
   * @return the View/Model
   */
  override def decorateForIndex(uri: String, entities: Seq[ModelEntity]): List[GetModels] = {
    entities.map(model => new GetModels(id = model.id,
      name = model.name,
      url = uri + "/" + model.id,
      entityType = model.entityType)).toList
  }
}
