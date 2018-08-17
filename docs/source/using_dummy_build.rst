
.. _dummy-build:

Dummy build
===========

In one terminal, start a server:

.. code-block:: sh

   bgd server start

In another terminal, send a request for work:

.. code-block:: sh

   bgd execute request-dummy

The stage should show as ``QUEUED`` as it awaits a bot to pick up the work:

.. code-block:: sh

   bgd execute list

Create a bot session:

.. code-block:: sh

   bgd bot dummy

Show the work as completed:

.. code-block:: sh

   bgd execute list
