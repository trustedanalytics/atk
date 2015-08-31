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

package org.trustedanalytics.atk.giraph.plugins.model.cf

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ ModelReference, GenericNewModelArgs }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Collaborative filtering recommend model - create a 'new' instance of this model
 */
@PluginDoc(oneLine = "Collaborative filtering recommend model.",
  extended = """**Collaborative Filtering**

Collaborative filtering is a technique that is widely used in recommendation
systems to suggest items (for example, products, movies,
articles) to potential users based on historical records of items that users
have purchased, rated, or viewed.
The |PACKAGE| provides implementations of collaborative filtering with either
Alternating Least Squares (ALS) or Conjugate Gradient Descent (CGD)
optimization methods.

Both methods optimize the cost function found in Y. Koren,
`Factorization Meets the Neighborhood: a Multifaceted Collaborative Filtering
Model <http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf>`__ in ACM KDD 2008.
For more information on optimizing using ALS see, Y. Zhou,
D. Wilkinson, R. Schreiber and R. Pan,
`Large-Scale Parallel Collaborative Filtering for the Netflix Prize
<http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.173.2797>`__ , 2008.

CGD provides a faster, more approximate optimization of the cost function and
should be used when memory is a constraint.

A typical representation of the preference matrix P in Giraph is a bi-partite
graph, where nodes at the left side represent a list of users and nodes at the
right side represent a set of items (for example, movies), and edges encode
the rating a user provided to an item.
To support training, validation and test, a common practice in machine
learning, each edge is also annotated by "TR", "VA" or "TE".

.. figure:: /ds_mlal_cf_1.*
    :align: center
    :width: 80 %

    A typical representation of the preference matrix P

Each node in the graph will be associated with a vector
:math:`\textstyle \overrightarrow {f_x}` of length :math:`k`, where :math:`k`
is the feature dimension specified by the user, and a bias term :math:`b_x`.
The predictions for item :math:`m_{j}`, from user :math:`u_{i}` care given by
dot product of the feature vector and the user vector, plus the item and user
bias terms:
/home/work/atk/engine-plugins/giraph-plugins/src/main/scala/org/trustedanalytics/atk/giraph/plugins/model/cf/CollaborativeFilteringNewPlugin.scala

.. math::

    r_{ij} = \overrightarrow {f_{ui}} \cdot \overrightarrow {f_{mj}} + b_{ui} \
    + b_{mj}

The parameters of the above equation are chosen to minimize the regularized
mean squared error between known and predicted ratings:

.. math::

    cost = \frac {\sum error^2} {n} + \lambda * \left( bias^2 + \sum f_k^2 \
    \right)

How this optimization is accomplished depends on whether the use uses the ALS
or CGD functions respectively.
It is recommended that the ALS method be used to solve collaborative filtering
problems.
The CGD method uses less memory than ALS, but it returns an approximate
solution to the objective function and should only be used in cases when
memory required for ALS is prohibitively high.


**Using ALS Optimization to Solve the Collaborative Filtering Problem**

ALS optimizes the vector :math:`\overrightarrow f_{*}` and the bias
:math:`b_{*}` alternatively between user profiles using least squares on users
and items.
On the first iteration, the first feature of each item is set to its average
rating, while the others are set to small random numbers.
The algorithm then treats the :math:`m` 's as constant and optimizes
:math:`u_{i}^{1},...,u_{i}^{k}` for each user, :math:`i`.
For an individual user, this is a simple ordinary least squares optimization
over the items that user has ranked.
Next, the algorithm takes the :math:`u` 's as constant and optimizes the
:math:`m_{j}^{1},...,m_{j}^{k}` for each item, :math:`j`.
This is again an ordinary least squares optimization predicting the user
rating of person that has ranked item :math:`j`.

At each step, the bias is computed for either items of users and the objective
function, shown below, is evaluated.
The bias term for an item or user, computed for use in the next iteration is
given by:

.. math::

    b = \frac{\sum error}{(1+\lambda)*n}

The optimization is said to converge if the change in the objective function
is less than the convergence_threshold parameter or the algorithm hits the
maximum number of :term:`supersteps`.

.. math::

    cost = \frac {\sum error^{2}}{n}+\lambda*\left(bias^{2}+\sum f_{k}^{2} \
    \right)

Note that the equations above omit user and item subscripts for generality.
The :math:`l_{2}` regularization term, lambda, tries to avoid overfitting by
penalizing the magnitudes of the parameters, and :math:`\lambda` is a tradeoff
parameter that balances the two terms and is usually determined by cross
validation (CV).

After the parameters :math:`\overrightarrow f_{*}` and :math:`b_{*}` are
determined, given an item :math:`m_{j}` the rating from user :math:`u_{i}` can
be predicted by the simple linear model:

.. math::

    r_{ij} = \overrightarrow {f_{ui}} \cdot \overrightarrow {f_{mj}} + b_{ui} \
    + b_{mj}

|
**Matrix Factorization based on Conjugate Gradient Descent (CGD)**

This is the Conjugate Gradient Descent (CGD) with Bias for collaborative
filtering algorithm.
Our implementation is based on the paper:

Y. Koren. Factorization Meets the Neighborhood: a Multifaceted Collaborative
Filtering Model.
In ACM KDD 2008. (Equation 5)
http://public.research.att.com/~volinsky/netflix/kdd08koren.pdf

This algorithm for collaborative filtering is used in :term:`recommendation
systems` to suggest items (products, movies, articles, and so on) to potential
users based on historical records of items that all users have purchased,
rated, or viewed.
The records are usually organized as a preference matrix P, which is a sparse
matrix holding the preferences (such as, ratings) given by users to items.
Similar to ALS, CGD falls in the category of matrix factorization/latent factor
model that infers user profiles and item profiles in low-dimension space, such
that the original matrix P can be approximated by a linear model.

This factorization method uses the conjugate gradient method for its
optimization subroutine.
For more on conjugate gradient descent in general, see:
http://en.wikipedia.org/wiki/Conjugate_gradient_method.

**The Mathematics of Matrix Factorization via CGD**

Matrix factorization by conjugate gradient descent produces ratings by using
the (limited) space of observed rankings to infer a user-factors vector
:math:`p_{u}` for each user  :math:`u`, and an item-factors vector
:math:`q_{i}` for each item :math:`i`, and then producing a ranking by user
:math:`u` of item :math:`i` by the dot-product :math:`b_{ui} + p_{u}^{T}q_{i}`
where :math:`b_{ui}` is a baseline ranking calculated as :math:`b_{ui} = \mu +
b_{u} + b_{i}`.

The optimum model is chosen to minimum the following sum, which penalizes
square distance of the prediction from observed rankings and complexity of the
model (through the regularization term):

.. math::
    \sum_{(u,i) \in {\mathcal{K}}} (r_{ui} - \mu - b_{u} - b_{i} - \
    p_{u}^{T}q_{i})^{2} + \lambda_{3}(||p_{u}||^{2} + ||q_{i}||^{2} + \
    b_{u}^{2} + b_{i}^{2})

Where:

    | :math:`r_{ui}` |EM| Observed ranking of item :math:`i` by user :math:`u`
    | :math:`{\mathcal{K}}` |EM| Set of pairs :math:`(u,i)` for each observed
      ranking of item :math:`i` by user :math:`u`
    | :math:`\mu` |EM| The average rating over all ratings of all items by all
      users.
    | :math:`b_{u}` |EM|  How much user :math:`u`'s average rating differs from
      :math:`\mu`.
    | :math:`b_{i}` |EM|   How much item :math:`i`'s average rating differs from
      :math:`\mu`
    | :math:`p_{u}` |EM|  User-factors vector.
    | :math:`q_{i}` |EM| Item-factors vector.
    | :math:`\lambda_{3}` |EM| A regularization parameter specified by the user.


This optimization problem is solved by the conjugate gradient descent method.
Indeed, this difference in how the optimization problem is solved is the
primary difference between matrix factorization by CGD and matrix factorization
by ALS.

**Comparison between CGD and ALS**

Both CGD and ALS provide recommendation systems based on matrix factorization;
the difference is that CGD employs the conjugate gradient descent instead of
least squares for its optimization phase.
In particular, they share the same bipartite graph representation and the same
cost function.

*   ALS finds a better solution faster - when it can run on the cluster it is
    given.
*   CGD has slighter memory requirements and can run on datasets that can
    overwhelm the ALS-based solution.

When feasible, ALS is a preferred solver over CGD, while CGD is recommended
only when the application requires so much memory that it might be beyond the
capacity of the system.
CGD has a smaller memory requirement, but has a slower rate of convergence and
can provide a rougher estimate of the solution than the more computationally
intensive ALS.

The reason for this is that ALS solves the optimization problem by a least
squares that requires inverting a matrix.
Therefore, it requires more memory and computational effort.
But ALS, a 2nd-order optimization method, enjoys higher convergence rate and is
potentially more accurate in parameter estimation.

On the otherhand, CGD is a 1.5th-order optimization method that approximates
the Hessian of the cost function from the previous gradient information
through N consecutive CGD updates.
This is very important in cases where the solution has thousands or even
millions of components.

**Usage**

The matrix factorization by CGD procedure takes a property graph, encoding a
biparite user-item ranking network, selects a subset of the edges to be
considered (via a selection of edge labels), takes initial ratings from
specified edge property values, and then writes each user-factors vector to its
user vertex in a specified vertex property name and each item-factors vector to
its item vertex in the specified vertex property name.
""")
class CollaborativeFilteringNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    val models = engine.models
    models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:collaborativefiltering_new")))
  }
}
