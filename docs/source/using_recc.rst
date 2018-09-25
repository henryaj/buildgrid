
.. _recc-client:

RECC client
===========

`RECC`_ is the *Remote Execution Caching Compiler*, an open source build tool
that wraps compiler command calls and forwards them to a remote build execution
service using the remote execution API (REAPI) v2.

.. note::

   There is no stable release of RECC yet. You'll have to `install it from
   sources`_.

.. _RECC: https://gitlab.com/bloomberg/recc
.. _install it from sources: https://gitlab.com/bloomberg/recc#installing-dependencies


.. _recc-configuration:

Configuration
-------------

RECC reads the configuration from its execution environment. You can get a
complete list of environment variables it accepts by running:

.. code-block:: sh

   recc --help

The variables are prefixed with ``RECC_``. The most important ones for remote
execution are:

- ``RECC_SERVER``: URI of the remote execution server.
- ``RECC_CAS_SERVER``: URI of the CAS server, defaults to ``RECC_SERVER``.
- ``RECC_INSTANCE``: name of the remote execution instance.

.. hint::

   ``RECC_VERBOSE=1`` can be set in order to enable verbose output.

As an example, in order to forward compile commands to the ``main`` instance of
the remote execution server available at ``controller.grid.build`` on port
``50051`` you should export:

.. code-block:: sh

   export RECC_SERVER=controller.grid.build:50051
   export RECC_INSTANCE=main


.. _recc-example:

Example build
-------------

RECC can be use with any existing software package respecting `GNU make common
variables`_ like ``CC`` for the C compiler or ``CXX`` for the C++ compiler.
We'll focus here on instructions on how to build the `GNU Hello`_ example
program using RECC and BuildGrid on your local machine.

First, you need to download the hello source package:

.. code-block:: sh

   wget https://ftp.gnu.org/gnu/hello/hello-2.10.tar.gz

Next, unpack it and change the current directory to the source root:

.. code-block:: sh

   tar xvf hello-2.10.tar.gz
   cd hello-2.10

.. hint::

   All the commands in the instructions below are expected to be executed from
   that root source directory (the GNU Hello project's root directory).

Before trying to build the hello example program, you'll have to setup and run a
BuildGrid server and bot. A minimal server's configuration is given below, paste
it in a ``server.conf`` file in the root directory:

.. literalinclude:: ./data/bazel-example-server.conf
   :language: yaml

This defines a single ``main`` server instance implementing a
``ContentAddressableStorage`` (CAS) + ``ByteStream`` service together with an
``Execution`` + ``ActionCache`` service, both using the same in-memory storage.
You can then start the BuildGrid server daemon using that configuration by
running:

.. code-block:: sh

   bgd server start server.conf

In order to perform the actual build work, you need to attach a worker bot to
that server for that ``main`` instance. RECC comes with its own ``reccworker``
bot implementation. However, BuildGrid's host-tools based bot should be enough
to build the hello example program. Once you've make sure that your machine has
``gcc`` installed, run:

.. code-block:: sh

   bgd bot --remote=http://localhost:50051 --parent=main host-tools

The ``--remote`` option is used to specify the server location (running on the
same machine here, and listening to port 50051). The ``--parent`` option is used
to specify the server instance you expect the bot to be attached to. Refer to
the :ref:`CLI reference section <invoking-bgd-bot-host-tools>` for command
line interface details.

The BuildGrid server is now ready to accept jobs and execute them. RECC's
:ref:`configuration <bazel-configuration>` needs to be defined as environment
variables. Define minimal configuration by running:

.. code-block:: sh

   export RECC_SERVER=localhost:50051
   export RECC_INSTANCE=main

This points RECC to the ``main`` remote execution server instance at
``localhost:50051``.

GNU Hello is using `The Autotools`_ as a build system, so first, you need to
configure your build. Run:

.. code-block:: sh

   ./configure

You can finally build the hello example program, using RECC by running:

.. code-block:: sh

   make CC="recc cc"

You can verify that the example program has been successfully built by running
the generated executable. Simply invoke:

.. code-block:: sh

   ./hello

.. _GNU make common variables: https://www.gnu.org/software/make/manual/html_node/Implicit-Variables.html
.. _GNU Hello: https://www.gnu.org/software/hello
.. _The Autotools: https://www.gnu.org/software/automake/manual/html_node/Autotools-Introduction.html