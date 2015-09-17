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

package org.trustedanalytics.atk.domain.model

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/*
This comment includes data for LDA model:

Command Line Options
--------------------
|LDA| can be invoked in the |PACKAGE| using the function
``latent_dirichlet_allocation``.
It can take several parameters, each of which are explained below.
::

        latent_dirichlet_allocation(
                                    edge_value_property_list,
                                    input_edge_label_list,
                                    output_vertex_property_list,
                                    vertex_type_property_key,
                                    vector_value,
                                    max_supersteps = 20,
                                    alpha = 0.1,
                                    beta = 0.1,
                                    convergence_threshold = 0.001,
                                    evaluation_cost = False,
                                    max_value,
                                    min_value,
                                    bidirectional_check,
                                    num_topics
                                    )

Parameters
----------

edge_value_property_list:
    Comma-separated String

    The edge properties containing the input edge values.
    We expect comma-separated list of property names if you use more than
    one edge property.
 
input_edge_label_list:
    Comma-separated String

    The name of edge label.
 
output_vertex_property_list:
    Comma-separated List

    The list of vertex properties to store output vertex values.
 
vertex_type:
    String

    The name of the vertex type.
 
vector_value:
    Boolean

    Denotes whether a vector can be passed as a vertex value.
 
max_supersteps:
    Integer (optional)

    The maximum number of :term:`supersteps` (iterations) that will be
    executed.
    Defaults to 20, but any positive integer is accepted.
 
alpha:
    Float (optional)

    The :term:`hyperparameter` for document-specific distribution over topics.
    Larger values imply that documents are assumed to cover topics more
    uniformly; smaller values imply documents are concentrated
    on a small subset of topics.
    Defaults to 0.1, but all positive floating-point numbers are
    acceptable.

beta:
    Float (optional)

    The :term:`hyperparameter` for word-specific distribution over topics.
    Larger values imply topics contain all words more uniformly, while
    smaller values imply topics are concentrated on a smaller subset of
    words.

    Defaults to 0.1, but all positive floating-point numbers are
    acceptable.
 
convergence_threshold:
    Float (optional)

    Sets the maximum change for convergence to be achieved.
    Defaults to 0.001, but floating-point values greater than or equal to
    zero are acceptable.

evaluate_cost:
    String (optional)

    "True" turns on cost evaluation, and "False" turns it off.
    It is relatively expensive for |LDA| to evaluate cost function.
    For time- critical applications, this option allows user to turn off
    cost function evaluation.
    Defaults to "False".
 
max_val:
    Float (optional)

    The maximum value for edge weights.
    If an edge weight is larger than this, the algorithm will throw an
    exception and terminate.
    This option is used for graph integrity checks.
    The defaults to infinity, but all floating-point numbers are
    acceptable.
 
min_val:
    Float (optional)

    The minimum value for edge weights.
    If an edge weight is smaller than this, the algorithm will throw an
    exception and terminate.
    This option is used for graph integrity check.
    Negative infinity is the default value, but all floating-point numbers
    are acceptable.

bidirectional_check:
    Boolean (optional)

    Turns bidirectional check on and off.
    |LDA| expects a bipartite input graph, so each edge should be
    bi-directional.
    This option is mainly for graph integrity check.

num_topics:
    Integer (optional)

    The number of topics to identify in the |LDA| model.
    Using fewer topics will speed up the computation, but the extracted
    topics will be less specific; using more topics will result
    in more computation but lead to more specific topics.
    The default value is 10, but all positive integers are accepted.
*/
case class GenericNewModelArgs(dummyModelRef: ModelReference,
                               @ArgDoc("""User supplied name.""") name: Option[String] = None)
