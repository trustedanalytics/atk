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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, CommandPlugin, PluginDoc }

// implicits needed for json conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAJsonProtocol._

@PluginDoc(oneLine = "Create a 'new' instance of an Autoregressive Integrated Moving Average (ARIMA) model.",
  extended = """
An autoregressive integrated moving average (ARIMA) [1]_ model is a
generalization of an autoregressive moving average (ARMA) model.
These models are fitted to time series data either to better understand
the data or to predict future points in the series (forecasting).
Non-seasonal ARIMA models are generally denoted ARIMA (p,d,q) where
parameters p, d, and q are non-negative integers, p is the order of the
Autoregressive model, d is the degree of differencing, and q is the order
of the Moving-average model.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Autoregressive_integrated_moving_average
    """,
  returns = "A new instance of ARIMAModel")
class ARIMANewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  override def name: String = "model:arima/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:arima")))
  }
}
