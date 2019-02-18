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

BuildGrid only supports ``python3 >= 3.5.3`` but has no system requirements.
Main Python dependencies, automatically handled during installation, include:

- `boto3`_: the Amazon Web Services (AWS) SDK for Python.
- `click`_: a Python composable command line library.
- `grpcio`_: Google's `gRPC`_ Python interface.
- `janus`_: a mixed sync-async Python queue.
- `protobuf`_: Google's `protocol-buffers`_ Python interface.
- `PyYAML`_: a YAML parser and emitter for Python.

.. _boto3: https://pypi.org/project/boto3
.. _click: https://pypi.org/project/click
.. _grpcio: https://pypi.org/project/grpcio
.. _gRPC: https://grpc.io
.. _janus: https://pypi.org/project/janus
.. _protobuf: https://pypi.org/project/protobuf
.. _protocol-buffers: https://developers.google.com/protocol-buffers
.. _PyYAML: https://pypi.org/project/PyYAML


.. _install-host-source-install:

Install from sources
~~~~~~~~~~~~~~~~~~~~

BuildGrid has ``setuptools`` support. We recommend installing it in a dedicated
`virtual environment`_. In order to do so in an environment named ``env``
placed in the source tree, run:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git
   cd buildgrid
   python3 -m venv env
   env/bin/python -m pip install --upgrade setuptools pip wheel
   env/bin/python -m pip install --editable .

.. hint::

   Once created, the virtual environment can be *activated* by sourcing the
   ``env/bin/activate`` script. In an activated terminal session, simply run
   ``deactivate`` to later *deactivate* it.

Once completed, you can check that installation succeed by locally starting the
BuildGrid server with default configuration. Simply run:

.. code-block:: sh

   env/bin/bgd server start data/config/default.conf -vvv

.. note::

   The ``setup.py`` script defines three extra targets, ``auth``, ``docs`` and
   ``tests``. They declare required dependency for, respectively, authentication
   and authorization management, generating documentation and running
   unit-tests. They can be use as helpers for setting up a development
   environment. To use them run:

   .. code-block:: sh

      env/bin/python -m pip install --editable ".[auth,docs,tests]"

.. _virtual environment: https://docs.python.org/3/library/venv.html


.. install-docker:

Install through Docker
----------------------

BuildGrid comes with Docker support for local development use-cases.

.. caution::

   The Docker manifests are intended to be use for **local development only**.
   Do **not** use them in production.

Please consult the `Get Started with Docker`_ guide if you are looking for
instructions on how to setup Docker on your machine.

.. _`Get Started with Docker`: https://www.docker.com/get-started


.. _install-docker-build:

Docker build
~~~~~~~~~~~~

BuildGrid ships a ``Dockerfile`` manifest for building images from source using
``docker build``. In order to produce a ``buildgrid:local`` base image, run:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git
   cd buildgrid
   docker build --tag buildgrid:local .

.. note::

   The image built will contain the Python sources, including example
   configuration files. The main endpoint is the ``bgd`` CLI tools and the
   default command shall run the BuildGrid server loading default configuration.

Once completed, you can check that build succeed by locally starting in a
container the BuildGrid server with default configuration. Simply run:

.. code-block:: sh

   docker run --interactive --publish 50051:50051 buildgrid:local

.. hint::

   You can run any of the BuildGrid CLI tool using that image, simply pass extra
   arguments to ``docker run`` the same way you would pass them to ``bgd``.

    Bear in mind that whenever the source code or the configuration files are
    updated, you **must** re-build the image.


.. _install-docker-compose:

Docker Compose
~~~~~~~~~~~~~~

BuildGrid ships a ``docker-compose.yml`` manifest for building and running a
grid locally using ``docker-compose``. In order to produce a
``buildgrid:local`` base image, run:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git
   cd buildgrid
   docker-compose build

Once completed, you can start a minimal grid by running:

.. code-block:: sh

   docker-compose up

.. note::

   The grid is composed of three containers:

   - An execution and action-cache service available at
     ``http://localhost:50051``.
   - An CAS service available at ``http://localhost:50052``.
   - A single ``local`` instance with one host-tools based worker bot attached.

.. hint::

   You can spin up more bots by using ``docker-compose`` scaling capabilities:

   .. code-block:: sh

      docker-compose up --scale bots=12

.. hint::

   The contained services configuration files are bind mounted into the
   container, no need to rebuild the base image on configuration update.
   Configuration files are read from:

   - ``data/config/controller.conf`` for the execution service.
   - ``data/config/storage.conf`` for the CAS and action-cache service.
