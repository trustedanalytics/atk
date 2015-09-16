Examples
--------
.. only:: html

    For example, if your right-side vertices are users,
    and you want to get movie recommendations for user 8941,
    the command to use is:

    .. code::

        >>> g.query.recommend(right_vertex_id_property_key='user', left_vertex_id_property_key='movie_name', vertex_type="R", vertex_id = "8941")

    The expected output of recommended movies looks like this:

    .. code::

        >>> {u'recommendation': [{u'vertex_id': u'once_upon_a_time_in_mexico', u'score': 3.831419911100037, u'rank': 1},{u'vertex_id': u'nocturne_1946', u'score': 3.541907655192171, u'rank': 2},{u'vertex_id': u'red_hot_skate_rock', u'score': 3.2573571020389407, u'rank': 3}]}

.. only:: latex

    For example, if your right-side vertices are users,
    and you want to get movie recommendations for user 8941,
    the command to use is:

    .. code::

        >>> g.query.recommend(
        ...     right_vertex_id_property_key='user',
        ...     left_vertex_id_property_key='movie_name',
        ...     vertex_type="R",
        ...     vertex_id = "8941")

    The expected output of recommended movies looks like this:

    .. code::

        {u'recommendation':
        [{u'vertex_id': u'once_upon_a_time_in_mexico',
          u'score': 3.831419911100037, u'rank': 1},
         {u'vertex_id': u'nocturne_1946',
          u'score': 3.541907655192171, u'rank': 2},
         {u'vertex_id': u'red_hot_skate_rock',
          u'score': 3.2573571020389407, u'rank': 3}]}


