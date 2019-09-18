
.. _configuration:

Configuration
=============

The details of how to tune BuildGrid's configuration.


.. _configuration-location:

Configuration location
----------------------

Unless a configuration file is explicitly specified on the command line when
invoking `bgd`, BuildGrid will always attempt to load configuration resources
from ``$XDG_CONFIG_HOME/buildgrid``. On most Linux based systems, the location
will be ``~/.config/buildgrid``.

This location is refered as ``$CONFIG_HOME`` is the rest of the document.


.. _tls-encryption:

TLS encryption
--------------

Every BuildGrid gRPC communication channel can be encrypted using SSL/TLS. By
default, the BuildGrid server will try to setup secure gRPC endpoints and return
in error if that fails. You must specify ``--allow-insecure`` explicitly if you
want it to use non-encrypted connections.

The TLS protocol handshake relies on an asymmetric cryptography system that
requires the server and the client to own a public/private key pair. BuildGrid
will try to load keys from these locations by default:

- Server private key: ``$CONFIG_HOME/server.key``
- Server public key/certificate: ``$CONFIG_HOME/server.crt``
- Client private key: ``$CONFIG_HOME/client.key``
- Client public key/certificate: ``$CONFIG_HOME/client.crt``


Server key pair
~~~~~~~~~~~~~~~

The TLS protocol requires a key pair to be used by the server. The following
example generates a self-signed key ``server.key``, which requires clients to
have a copy of the server certificate ``server.crt``. You can of course use a
key pair obtained from a trusted certificate authority instead.

.. code-block:: sh

   openssl req -new -newkey rsa:4096 -x509 -sha256 -days 3650 -nodes -batch -subj "/CN=localhost" -out server.crt -keyout server.key


Client key pair
~~~~~~~~~~~~~~~

If the server requires authentication in order to be granted special permissions
like uploading to CAS, a client side key pair is required. The following example
generates a self-signed key ``client.key``, which requires the server to have a
copy of the client certificate ``client.crt``.

.. code-block:: sh

   openssl req -new -newkey rsa:4096 -x509 -sha256 -days 3650 -nodes -batch -subj "/CN=client" -out client.crt -keyout client.key


.. _persisting-state:

Persisting Internal State
-------------------------

BuildGrid's Execution and Bots services can be configured to store their internal
state (such as the job queue) in an external data store of some kind. At the moment
the only supported type of data store is any SQL database with a driver supported
by SQLALchemy.

This makes it possible to restart a BuildGrid cluster without affecting the internal
state, alleviating concerns about having to finish currently queued or executing work
before restarting the scheduler or else accept losing track of that work. Upon
restarting, BuildGrid will load the jobs it previously knew about from the data
store, and recreate its internal state. Previous connections will need to be recreated
however, which can be done by a client sending a WaitExecution request with the
relevant operation name.


SQL Database
~~~~~~~~~~~~

The SQL data store implementation uses SQLAlchemy to connect to a database for storing
the job queue and related state.

There are database migrations provided, and BuildGrid can be configured to
automatically run them when connecting to the database. Alternatively, this can
be disabled and the migrations can be executed manually using Alembic.

An example execution service configuration block, using SQLite:

.. code-block:: yaml

    - !execution
      storage: *data-store
      action-cache: *build-cache
      action-browser-url: http://localhost:8080
      data-store:
        type: sql
        connection_string: sqlite:///./example.db
        automigrate: yes

A similar configuration block, using PostgreSQL:

.. code-block:: yaml

    - !execution
      storage: *data-store
      action-cache: *build-cache
      action-browser-url: http://localhost:8080
      data-store:
        type: sql
        connection_string: postgresql://username:password@sql_server/database_name
        automigrate: yes
 
With ``automigrate: no``, the migrations can be run by cloning the `git repository`_,
modifying the ``sqlalchemy.url`` line in ``alembic.ini`` to match the
``connection_string`` in the configuration, and executing

.. code-block:: sh

    tox -e venv -- alembic --config ./alembic.ini upgrade head

in the root directory of the repository. The docker-compose files in the
`git repository`_ offer an example approach for PostgreSQL.

.. hint::

   For the creation of the database and depending on the permissions and database config,
   you may need to create and initialize the database before Alembic can create all the
   tables for you.

   If Alembic fails to create the tables because it cannot read or create the ``alembic_version`` table,
   you could use the following SQL command:

   .. code-block:: sql

       CREATE TABLE alembic_version (
         version_num VARCHAR(32) NOT NULL,
         CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))

.. _git repository: https://gitlab.com/BuildGrid/buildgrid
