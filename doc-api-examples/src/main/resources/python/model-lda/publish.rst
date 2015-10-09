Examples
--------
Publish a trained Lda Model for scoring

.. only:: html

    .. code::

        >>> my_model = ta.LdaModel(name='MyLdaModel')
        >>> my_model.train(frame, 'doc_id', 'word_id', 'word_count', max_iterations = 3, num_topics = 2)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>

.. only:: latex

    .. code::

        >>> my_model = ta.LdaModel(name='MyLdaModel')
        >>> my_model.train(frame, 'doc_id', 'word_id', 'word_count', max_iterations = 3, num_topics = 2)
        >>> my_model.publish()
        <Path in HDFS to model's tar file>