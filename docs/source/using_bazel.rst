
.. _bazel-client:

Bazel client
============

`Bazel`_ is a *“fast, scalable, multi-language and extensible build system”*
that supports remote build execution using the remote execution API (REAPI) v2
since its `0.17 release`_.

.. _Bazel: https://bazel.build
.. _0.17 release: https://blog.bazel.build/2018/09/14/bazel-0.17.html


.. _bazel-configuration:

Configuration
-------------

Bazel accepts many options that can either be specified as command line
arguments when invoking the ``bazel`` tool or stored in a `.bazelrc`_
configuration file. In order to activate remote build execution, the
``bazel build`` subcommand needs to be given specific `build options`_,
including:

- ``--remote_executor``: remote execution endpoint's location, ``{host}:{port}``.
- ``--remote_instance_name``: remote execution instance's name.
- ``--spawn_strategy``: action execution method.
- ``--genrule_strategy``: `genrules`_ execution method.

Spawn and genrule strategies need to be set to ``remote`` for remote execution.

As an example, in order to activate remote execution on the ``main`` instance of
the remote execution server available at ``controller.grid.build`` on port
``50051`` you should amend your ``.bazelrc`` with:

.. code-block:: sh

   build --spawn_strategy=remote --genrule_strategy=remote --remote_executor=controller.grid.build:50051 --remote_instance_name=main

.. _.bazelrc: https://docs.bazel.build/versions/master/user-manual.html#bazelrc
.. _build options: https://docs.bazel.build/versions/master/command-line-reference.html#build-options
.. _genrules: https://docs.bazel.build/versions/master/be/general.html#genrule


.. _bazel-example:

Example build
-------------

The `bazel-examples`_ repository contains example Bazel workspaces that can be
used for testing purpose. We'll focus here on instructions on how to build the
`stage3 CPP example`_ running Bazel and BuildGrid on your local machine,
compiling the C++ source code using host-tools.

First, you need to checkout the bazel-examples repository sources:

.. code-block:: sh

   git clone https://github.com/bazelbuild/examples.git bazel-examples

Next, change the current directory to the Bazel workspace root:

.. code-block:: sh

   cd bazel-examples/cpp-tutorial/stage3

.. hint::

   All the commands in the instructions below are expected to be executed from
   that root directory (the stage3 example workspace's root directory).

Before running Bazel and building the example workspace, you'll have to setup
and run a BuildGrid server and bot. A minimal server's configuration is given
below, paste it in a ``server.conf`` file in the root directory:

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
that server for that ``main`` instance. A simple host-tools based bot should be
enough to build the stage3 CPP example. Once you've make sure that your machine
has ``gcc`` installed, run:

.. code-block:: sh

   bgd bot --remote=http://localhost:50051 --parent=main host-tools

The ``--remote`` option is used to specify the server location (running on the
same machine here, and listening to port 50051). The ``--parent`` option is used
to specify the server instance you expect the bot to be attached to. Refer to
the :ref:`CLI reference section <invoking-bgd-bot-host-tools>` for command
line interface details.

The BuildGrid server is now ready to accept jobs and execute them. Bazel needs
some :ref:`configuration <bazel-configuration>` in order to run remote builds.
Below are the minimal build options you should use, paste them in a ``.bazelrc``
file in the root directory:

.. code-block:: sh

   build --spawn_strategy=remote --genrule_strategy=remote --remote_executor=localhost:50051 --remote_instance_name=main

This activates Bazel's remote execution mode and points to the ``main`` remote
execution server instance at ``localhost:50051``.

You can finally have Bazel to build the example workspace by running:

.. code-block:: sh

   bazel build //main:hello-world

You can verify that the example has been successfully built by running the
generated executable. Simply invoke:

.. code-block:: sh

   ./bazel-bin/main/hello-world

.. _bazel-examples: https://github.com/bazelbuild/examples
.. _stage3 CPP example: https://github.com/bazelbuild/examples/tree/master/cpp-tutorial/stage3