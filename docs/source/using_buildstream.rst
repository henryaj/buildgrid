
.. _buildstream-client:

BuildStream client
==================

`BuildStream`_ is a free software tool for building and integrating software
stacks. It supports remote build execution using the remote execution API
(REAPI) v2. The project's documentation has a detailed section about its
`remote execution subsystem architecture`_ that you are very recommanded to
read first.

.. note::

   There is no stable release of BuildStream with support for remote execution
   yet. You'll have to `install it from sources`_ in order to build with
   remote execution.

.. _BuildStream: https://buildstream.build
.. _remote execution subsystem architecture: https://buildstream.gitlab.io/buildstream/arch_remote_execution.html
.. _install it from sources: https://buildstream.build/source_install.html


.. _buildstream-configuration:

Configuration
-------------

BuildStream uses `YAML`_ for build definition and configuration. It has two
levels of configuration: user and project level. `User-level configuration`_ is
stored in your ``buildstream.conf`` file while `project-level configuration`_
is defined in each project's ``project.conf`` file.

.. note::

   At the moment, remote execution can only be configured at project-level.

.. _YAML: http://yaml.org
.. _User-level configuration: https://buildstream.gitlab.io/buildstream/using_config.html
.. _project-level configuration: https://buildstream.gitlab.io/buildstream/format_project.html


Project configuration
~~~~~~~~~~~~~~~~~~~~~

In order to activate remote build execution at project-level, the project's
``project.conf`` file must declare two specific configuration nodes:

- ``artifacts`` for `remote cache endpoint details`_.
- ``remote-execution`` for `remote execution endpoint details`_.

.. important::

   BuildStream does not support multi-instance remote execution servers and will
   always submit remote execution request omitting the instance name parameter.
   Thus, you must declare an unnamed `''` instance  in your server configuration
   to workaround this.

.. important::

   If you are using BuildStream's artifact server, the server instance pointed
   by the ``storage-service`` key **must** accept pushes from your client for
   remote execution to be possible.

.. _remote cache endpoint details: https://buildstream.gitlab.io/buildstream/format_project.html#artifact-server
.. _remote execution endpoint details: https://buildstream.gitlab.io/buildstream/format_project.html#remote-execution


BuildBox tool
~~~~~~~~~~~~~

BuildStream performs builds in a `sandbox`_ on top of a project-defined
environment, not relying on any host-tools. BuildGrid supports this kind of
build using the ``buildbox`` bot, a specific type of bot relying on `BuildBox`_
for build execution.

BuildBox can execute build commands in a sandbox on top of an environment
constructed from provided sources using `FUSE`_. It also uses `bubblewrap`_
for sandboxing without requiring root privileges.

BuildBox being a rather young project, it isn't packaged yet and you'll have to
build it from source. You may want follow the `manual instructions`_ or you can
build it with BuildStream using the `dedicated integration project`_ (recommanded).

.. important::

   Whatever the method you use to install BuildBox, you also have to install
   bubblewrap along, minimum required version being `0.1.8`_.

.. _sandbox: https://buildstream.gitlab.io/buildstream/additional_sandboxing.html
.. _BuildBox: https://gitlab.com/BuildStream/buildbox
.. _FUSE: https://en.wikipedia.org/wiki/Filesystem_in_Userspace
.. _bubblewrap: https://github.com/projectatomic/bubblewrap
.. _manual instructions: https://gitlab.com/BuildStream/buildbox/blob/master/INSTALL.rst
.. _dedicated integration project: https://gitlab.com/BuildStream/buildbox-integration
.. _0.1.8: https://github.com/projectatomic/bubblewrap/releases/tag/v0.1.8


.. _buildstream-example:

Example build
-------------

The BuildStream repository contains `example projects`_ used for testing purpose
in the project's `usage documentation section`_. We'll focus here on
instructions on how to build the `autotools example`_ running BuildStream and
BuildGrid on your local machine, compiling the `GNU Automake`_ hello example
program in a sandbox on top of a minimal `Alpine Linux`_ environment.

First, you need to checkout the buildstream repository sources:

.. code-block:: sh

   git clone https://gitlab.com/BuildStream/buildstream.git

Next, change the current directory to the BuildStream project root:

.. code-block:: sh

   cd buildstream/doc/examples/autotools

.. hint::

   All the commands in the instructions below are expected to be executed from
   that root directory (the autotools example project's root directory).

Before running BuildStream and building the example project, you'll have to setup
and run a BuildGrid server and bot. A minimal server's configuration is given
below, paste it in a ``server.conf`` file in the root directory:

.. literalinclude:: ./data/buildstream-example-server.conf
   :language: yaml

This defines a single unnamed server instance implementing a
``ContentAddressableStorage`` (CAS) + ``Reference`` + ``ByteStream`` service
together with an ``Execution`` + ``ActionCache`` service, both using the
same in-memory storage. You can then start the BuildGrid server daemon using
that configuration by running:

.. code-block:: sh

   bgd server start server.conf

In order to perform the actual build work, you need to attach a ``buildbox``
worker bot to that server for that unnamed instance. Once you've make sure
that the ``buildbox`` tool is functional on your machine (refer to
:ref:`configuration <buildstream-configuration>`), run:

.. code-block:: sh

   bgd bot --remote=http://localhost:50051 --parent= buildbox --fuse-dir=fuse --local-cas=cache

The ``--remote`` option is used to specify the server location (running on the
same machine here, and listening to port 50051). The ``--parent`` option is
used to specify the server instance you expect the bot to be attached to (empty
here). ``--fuse-dir`` and ``--local-cas`` are specific to the ``buildbox`` bot
and respectively specify the sandbox mount point and local CAS cache locations.
Refer to the :ref:`CLI reference section <invoking-bgd-bot-buildbox>` for
for command line interface details.

The BuildGrid server is now ready to accept jobs and execute them. The example
project needs some :ref:`configuration <buildstream-configuration>` tweaks in
order to be build remotely. Below is the configuration fragment you should
append at the end of the ``project.conf`` file from the root directory:

.. code-block:: yaml

   artifacts:
     url: http://localhost:50051
     push: true

   remote-execution:
     execution-service:
       url: http://localhost:50051
     storage-service:
       url: http://localhost:50051
       client-key: ''
       client-cert: ''
       server-cert: ''
     action-cache-service:
       url: http://localhost:50051

This activates BuildGrid's remote execution mode and points to the unnamed
remote execution server instance at ``localhost:50051``.

You can finally have BuildStream to build the example project by running:

.. code-block:: sh

   bst build hello.bst

You can verify that the example has been successfully built by running the
generated executable. Simply invoke:

.. code-block:: sh

   bst shell hello.bst -- hello

.. _example projects: https://gitlab.com/BuildStream/buildstream/tree/master/doc/examples
.. _usage documentation section: http://buildstream.gitlab.io/buildstream/main_using.html
.. _autotools example: https://gitlab.com/BuildStream/buildstream/tree/master/doc/examples/autotools
.. _GNU Automake: https://www.gnu.org/software/automake
.. _Alpine Linux: https://www.alpinelinux.org
