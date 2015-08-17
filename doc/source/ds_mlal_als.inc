.. index::
    single: ALS
    single: optimization
    single: collaborative filtering
    pair: algorithm; ALS

Using ALS Optimization to Solve the Collaborative Filtering Problem
===================================================================

ALS optimizes the vector :math:`\overrightarrow f_{*}` and the bias
:math:`b_{*}` alternatively between user profiles using least squares on users
and items.
On the first iteration, the first feature of each item is set to its average
rating, while the others are set to small random numbers.
The algorithm then treats the :math:`m` 's as constant and optimizes
:math:`u_{i}^{1},…,u_{i}^{k}` for each user, :math:`i`.
For an individual user, this is a simple ordinary least squares optimization
over the items that user has ranked.
Next, the algorithm takes the :math:`u` ’s as constant and optimizes the
:math:`m_{j}^{1},…,m_{j}^{k}` for each item, :math:`j`.
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

