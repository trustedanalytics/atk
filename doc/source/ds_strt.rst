.. _ds_strt:

===============
Getting Started
===============

To start using ATK on TAP, create a new Jupyter notebook instance and create a set of credentials to connect to ATK Server. The following steps will guide you through this process.   

For a more detailed set of instructions please visit the `Trusted Analytics Platform wiki pages <https://github.com/trustedanalytics/platform-wiki/wiki/Getting-Started-with-AT>`_.   
Note: The wiki page will be updated to stay in sync with the latest version of AT and TAP and might not always be consistent with AT version 0.7.0   

Creating a Jupyter Notebook Instance
------------------------------------

1.      From the *>Home* page, select *>Data Science* from the main menu, then select *>Jupyter*.
2.      Click inside form field labeled Instance Name and assign a name to your instance. 
3.      Select the button *>Create new Jupyter instance* to create a new instance.  The button is not available until a name has been provided in the available form field. 

Your new Jupyter instance has been created.  The new instance can be found by selecting *>Data Science* from the main menu, then by selecting the sub menu item *>Jupyter* and searching for the Instance Name you assigned in step 2. 

Working with the Analytics Toolkit from a Jupyter Notebook
----------------------------------------------------------

Create your credentials file: `Creating a Credentials File Example Notebook <https://github.com/trustedanalytics/jupyter-default-notebooks/blob/master/notebooks/examples/atk/create-credentials.ipynb>`_  

This step only needs to be taken the first time you attempt to use the Analytics Toolkit or when switching to a new instance of Analytics Toolkit.  
Creating credentials generates a file named myuser-cred.creds  
This file is necessary when writing scripts to connect to the Analytics Toolkit from Jupyter Notebooks.  

