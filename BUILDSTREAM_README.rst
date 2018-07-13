Temp Demo Instructions
======================

A quick guide to getting remote execution working with BuildStream. Please change URL and certifcates / keys to your own.

Downloaded and built::

  https://gitlab.com/BuildStream/buildbox

Copy build to bin/.

Checkout branch::

  https://gitlab.com/BuildGrid/buildgrid/tree/finn/fuse

Checkout branch::

  https://gitlab.com/BuildStream/buildstream/tree/jmac/source_pushing_experiments

Update to your URL::

  https://gitlab.com/BuildStream/buildstream/blob/jmac/source_pushing_experiments/buildstream/sandbox/_sandboxremote.py#L73

Start artifact server::

  bst-artifact-server --port 11001 --server-key server.key --server-cert server.crt --client-certs client.crt --enable-push /home/user/

Start bgd server::

  bgd server start

Run::

  bgd bot buildbox

Update project.conf in build area with::

  artifacts:
      url: https://localhost:11001
      server-cert: server.crt

      # Optional client key pair for authentication
      client-key: client.key
      client-cert: client.crt

      push: true

Run build with::

  bst build --track something.bst
