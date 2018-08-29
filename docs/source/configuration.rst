
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
