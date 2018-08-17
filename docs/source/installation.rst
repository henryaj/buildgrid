
.. _installation:

Installation
============

How to install BuildGrid onto your machine.

.. note::

   BuildGrid server currently only support *Linux*, *macOS* and *Windows*
   platforms are **not** supported.


.. _install-prerequisites:

Prerequisites
-------------

BuildGrid only supports ``python3 >= 3.5`` but has no system requirements. Main
Python dependencies, automatically handle during installation, includes:

- `boto3`_: the Amazon Web Services (AWS) SDK for Python.
- `click`_: a Python composable command line library.
- `grpcio`_: Google's `gRPC`_ Python interface.
- `protobuf`_: Google's `protocol-buffers`_ Python interface.

.. _boto3: https://pypi.org/project/boto3
.. _click: https://pypi.org/project/click
.. _grpcio: https://pypi.org/project/grpcio
.. _gRPC: https://grpc.io
.. _protobuf: https://pypi.org/project/protobuf
.. _protocol-buffers: https://developers.google.com/protocol-buffers


.. _source-install:

Install from sources
--------------------

BuildGrid has ``setuptools`` support. In order to install it to your home
directory, typically under ``~/.local``, simply run:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git && cd buildgrid
   pip3 install --user --editable .

Additionally, and if your distribution does not already includes it, you may
have to adjust your ``PATH``, in ``~/.bashrc``, with:

.. code-block:: sh

   export PATH="${PATH}:${HOME}/.local/bin"

.. note::

   The ``setup.py`` script defines two extra targets, ``docs`` and ``tests``,
   that declare required dependency for, respectively, generating documentation
   and running unit-tests. They can be use as helpers for setting up a
   development environment. To use them simply run:

   .. code-block:: sh

      pip3 install --user --editable ".[docs,tests]"
