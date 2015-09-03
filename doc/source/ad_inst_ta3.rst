.. _ad_inst_ta3:

Configuration Script
--------------------
.. only:: html

    Sample output with notes::

        #if the default is correct hit enter
        $ sudo ./config

        What port is Cloudera Manager listening on? defaults to "7180" if nothing is entered:
        What is the Cloudera Manager username? defaults to "admin" if nothing is entered:
        What is the Cloudera Manager password? defaults to "admin" if nothing is entered:

        No current SPARK_CLASSPATH set.
        Setting to:

        Deploying config   .   .   .   .   .   .   .   .   .   .   .   .
        Config Deployed

        You need to restart Spark service for the config changes to take affect.
        Would you like to restart now? Enter 'yes' to restart. defaults to 'no' if nothing is
        entered: yes
        Restarting Spark  .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .
        Restarted Spark


        What is the hostname of the database server? defaults to "localhost" if nothing is entered:
        What is the port of the database server? defaults to "5432" if nothing is entered:
        What is the name of the database? defaults to "ta_metastore" if nothing is entered:
        What is the database user name? defaults to "atkuser" if nothing is entered:

        #The dollar sign($) is not allowed in the password.
        What is the database password? The default password was randomly generated.
            Defaults to "****************************" if nothing is entered:

        Creating application.conf file from application.conf.tpl
        Reading application.conf.tpl
        Updating configuration
        Configuration created for Trusted Analytics ATK
        Configuring postgres access for  "atkuser"
        Initializing database:                                     [  OK  ]
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        CREATE ROLE
        0
        CREATE DATABASE
        0
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        initctl: Unknown instance:
        trustedanalytics-rest-server start/running, process 17484
        0
        Waiting for Trusted Analytics ATK server to restart
         .   .   .   .
        You are now connected to database "ta_metastore".
        INSERT 0 1
        0
        postgres is configured
        Trusted Analytics ATK is ready for use.

.. only:: latex

    Sample output with notes::

        #if the default is correct hit enter
        $ sudo ./config

        What port is Cloudera Manager listening on?
            defaults to "7180" if nothing is entered:
        What is the Cloudera Manager username?
            defaults to "admin" if nothing is entered:
        What is the Cloudera Manager password?
            defaults to "admin" if nothing is entered:

        No current SPARK_CLASSPATH set.
        Setting to:

        Deploying config   .   .   .   .   .   .   .   .   .   .   .   .
        Config Deployed

        You need to restart Spark service for the config changes to take affect.
        Would you like to restart now? Enter 'yes' to restart. defaults to 'no' if nothing is
        entered: yes
        Restarting Spark  .   .   .   .   .   .   .   .   .   .   .   .   .   .   .
        Restarted Spark


        What is the hostname of the database server?
            defaults to "localhost" if nothing is entered:
        What is the port of the database server?
            defaults to "5432" if nothing is entered:
        What is the name of the database?
            defaults to "ta_metastore" if nothing is entered:
        What is the database user name?
            defaults to "atkuser" if nothing is entered:

        #The dollar sign($) is not allowed in the password.
        What is the database password?
            The default password was randomly generated.
            Defaults to "****************************" if nothing is entered.

        Creating application.conf file from application.conf.tpl
        Reading application.conf.tpl
        Updating configuration
        Configuration created for Trusted Analytics ATK
        Configuring postgres access for  "atkuser"
        Initializing database:                                     [  OK  ]
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        CREATE ROLE
        0
        CREATE DATABASE
        0
        Stopping postgresql service:                               [  OK  ]
        Starting postgresql service:                               [  OK  ]
        0
        initctl: Unknown instance:
        trustedanalytics-rest-server start/running, process 17484
        0
        Waiting for Trusted Analytics ATK server to restart
         .   .   .   .
        You are now connected to database "ta_metastore".
        INSERT 0 1
        0
        postgres is configured
        Trusted Analytics ATK is ready for use.


