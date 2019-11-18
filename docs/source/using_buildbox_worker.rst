.. _using_buildbox_worker:

buildbox-worker
---------------

This is a guide to connecting a simple ``buildbox-worker`` to BuildGrid.
``buildbox-worker`` is a worker written in C++ that implements the Remote Workers API.

``buildbox-worker`` runs its underlying job using a pluggable "runner." For this example,
we will be using the ``buildbox-run-hosttools`` runner, which simply runs the command
you send to it directly with no sandboxing.

First, build ``buildbox-worker`` and ``buildbox-run-hosttools``. Instructions for each can be
found in the respective READMEs. As part of the build process, you will also need to build
``buildbox-common``, which has several dependencies that need to be installed.

Then, start a BuildGrid instance. If you have followed the installation guide and 
also have the BuildGrid source on your machine, you can use the default configuration:::

    bgd server start data/config/default.conf

Running the worker is pretty simple -- you just need to pass it the locations of the
BuildGrid server, BuildGrid CAS server, and ``buildbox-run-hosttools``. For example,
if your server has an execution service and a CAS service both located at localhost:50051,
you can invoke the worker like this::: 

    /path/to/buildbox-worker --bots-remote=http://localhost:50051 --cas-remote=http://localhost:50051 --buildbox-run=/path/to/buildbox-run-hosttools my_bot

If all goes well, you should have a functioning worker named ``my_bot`` that's ready to accept jobs.

Resources
---------

- `buildbox-worker`_
- `buildbox-run-hosttools`_
- `buildbox-common`_

.. _buildbox-worker: https://gitlab.com/BuildGrid/buildbox/buildbox-worker
.. _buildbox-run-hosttools: https://gitlab.com/BuildGrid/buildbox/buildbox-run-hosttools
.. _buildbox-common: https://gitlab.com/BuildGrid/buildbox/buildbox-common