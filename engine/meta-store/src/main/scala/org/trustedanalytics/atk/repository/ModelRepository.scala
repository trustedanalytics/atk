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

import org.trustedanalytics.atk.domain.model.{ ModelTemplate, ModelEntity }

import scala.util.Try

/**
 * Repository for models
 */
trait ModelRepository[Session] extends Repository[Session, ModelTemplate, ModelEntity] with NameableRepository[Session, ModelEntity] with GarbageCollectableRepository[Session, ModelEntity] {

  /**
   * Return all the models
   * @param session current session
   * @return all the models
   */
  def scanAll()(implicit session: Session): Seq[ModelEntity]

  /** update a model entity as Dropped */
  def dropModel(graph: ModelEntity)(implicit session: Session): Try[ModelEntity]

  /** gets sequence of all models with status Dropped */
  def droppedModels(implicit session: Session): Seq[ModelEntity]
  /**
   * Return list of model entities (without data) for all Active model entities with a name
   * @param session current session
   */
  def scanNamedActiveModelsNoData()(implicit session: Session): Seq[ModelEntity]

  /** get ModelEntity by name without data (extra to the normal entity repo) */
  def lookupByNameNoData(name: Option[String])(implicit session: Session): Option[ModelEntity]

  /** get ModelEntity without data (extra to the normal entity repo) */
  def lookupNoData(id: Long)(implicit session: Session): Option[ModelEntity]
}
