
.. _server-configuration:

Server configuration
====================

BuildGrid's server YAML configuration format details.

.. hint::

   In order to spin-up a server instance using a given ``server.conf``
   configuration file, run:

   .. code-block:: sh

      bgd server start server.conf

   Please refer to the :ref:`CLI reference section <invoking-bgd-server>` for
   command line interface details.


.. _server-config-reference:

Reference configuration
-----------------------

Below is an example of the full configuration reference:

.. literalinclude:: ../../buildgrid/_app/settings/reference.yml
   :language: yaml


.. _server-config-parser:

Parser API
----------

The tagged YAML nodes in the :ref:`reference above <server-config-reference>`
are handled by the YAML parser using the following set of objects:

.. automodule:: buildgrid._app.settings.parser
    :members:
    :undoc-members:
    :show-inheritance:
