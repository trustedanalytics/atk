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

package org.trustedanalytics.atk.domain

/**
 * Arguments needed to create a new entity
 *
 * @param name - e.g. for the entity type "model:logistic_regression", the subtype is "logistic_regression"
 * @param entityType - e.g. for the entity type "model:logistic_regression", the subtype is "logistic_regression"
 * @param description - optional description of the particular entity or why it was created
 */
case class CreateEntityArgs(name: Option[String] = None, entityType: Option[String] = None, description: Option[String] = None) {
  require(name != null)
  require(entityType != null)
  require(description != null)
  Naming.validateAlphaNumericUnderscoreOrNone(name)
}
