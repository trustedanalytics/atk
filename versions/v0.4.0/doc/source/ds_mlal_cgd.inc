.. index::
    single: matrix factorization
    single: CGD
    single: conjugate gradient descent

Matrix Factorization based on Conjugate Gradient Descent (CGD)
==============================================================

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

.. index::
    pair: algorithm; Conjugate Gradient Descent

The Mathematics of Matrix Factorization via CGD
-----------------------------------------------

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

    | :math:`r_{ui}` – Observed ranking of item :math:`i` by user :math:`u`
    | :math:`{\mathcal{K}}` – Set of pairs :math:`(u,i)` for each observed
      ranking of item :math:`i` by user :math:`u`
    | :math:`\mu` – The average rating over all ratings of all items by all
      users.
    | :math:`b_{u}` –  How much user :math:`u`'s average rating differs from
      :math:`\mu`.
    | :math:`b_{i}` –   How much item :math:`i`'s average rating differs from
      :math:`\mu`
    | :math:`p_{u}` –  User-factors vector.
    | :math:`q_{i}` – Item-factors vector.
    | :math:`\lambda_{3}` – A regularization parameter specified by the user.


This optimization problem is solved by the conjugate gradient descent method.
Indeed, this difference in how the optimization problem is solved is the
primary difference between matrix factorization by CGD and matrix factorization
by ALS.

Comparison between CGD and ALS
------------------------------

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

Usage
~~~~~

The matrix factorization by CGD procedure takes a property graph, encoding a
biparite user-item ranking network, selects a subset of the edges to be
considered (via a selection of edge labels), takes initial ratings from
specified edge property values, and then writes each user-factors vector to its
user vertex in a specified vertex property name and each item-factors vector to
its item vertex in the specified vertex property name.
