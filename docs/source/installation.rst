.. _installation:

Installation
============

.. _install-on-host:

Installation onto host machine
------------------------------

How to install BuildGrid directly onto your machine.

.. note::

   BuildGrid server currently only support *Linux*. *macOS* and *Windows*
   platforms are **not** supported.


.. _install-host-prerequisites:

Prerequisites
~~~~~~~~~~~~~

BuildGrid only supports ``python3 >= 3.5`` but has no system requirements. Main
Python dependencies, automatically handled during installation, include:

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


.. _install-host-source-install:

Install from sources
~~~~~~~~~~~~~~~~~~~~

BuildGrid has ``setuptools`` support. In order to install it to your home
directory, typically under ``~/.local``, simply run:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git && cd buildgrid
   pip3 install --user --editable .

Additionally, and if your distribution does not already include it, you may
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



.. install-docker:

Installation through docker
---------------------------

How to build a Docker image that runs BuildGrid.

.. _install-docker-prerequisites:

Prerequisites
~~~~~~~~~~~~~

A working Docker installation. Please consult `Docker's Getting Started Guide`_ if you don't already have it installed.

.. _`Docker's Getting Started Guide`: https://www.docker.com/get-started


.. _install-docker-build:

Docker Container from Sources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To clone the source code and build a Docker image, simply run:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git && cd buildgrid
   docker build -t buildgrid_server .

.. note::

   The image built will contain the contents of the source code directory, including
   configuration files.
   
.. hint::

    Whenever the source code is updated or new configuration files are made, you need to re-build 
    the image.

After building the Docker image, to run BuildGrid using the default configuration file 
(found in `buildgrid/_app/settings/default.yml`), simply run:

.. code-block:: sh

   docker run -i -p 50051:50051 buildgrid_server

.. note::

    To run BuildGrid using a different configuration file, include the relative path to the
    configuration file at the end of the command above. For example, to run the default 
    standalone CAS server (without an execution service), simply run:

       .. code-block:: sh

            docker run -i -p 50052:50052 buildgrid_server buildgrid/_app/settings/cas.yml

