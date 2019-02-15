.. _cas-server:

CAS server
==========

It is possible to configure BuildGrid with just a Content Addressable Storage service.

.. note::

   This service can be equivalent to `BuildStream's Artifact Server`_ if the `Reference Storage Service`_ is included.

.. _cas-configuration:

Configuration
-------------

Here is an example project configuration. It also implements an optional API called the `Reference Storage Service`_, which if used, allows the user to store a ``Digest`` behind a user defined ``key``.

.. literalinclude:: ./data/cas-example-server.conf
   :language: yaml

.. hint::

   Use ``- name: ""`` if using with BuildStream, as instance names are not supported for that tool yet.

This defines a single ``main`` instance of the ``CAS``, ``Bytestream`` and ``Reference Storage`` service on port ``55051``. It is backed onto disk storage and will populate the folder ``$HOME/cas``. To start the server, simply type into your terminal:

.. code-block:: sh

   bgd server start data/config/artifacts.conf

The server should now be available to use.

.. _BuildStream's Artifact Server: https://buildstream.gitlab.io/buildstream/install_artifacts.html
.. _Reference Storage Service: https://gitlab.com/BuildGrid/buildgrid/blob/master/buildgrid/_protos/buildstream/v2/buildstream.proto
