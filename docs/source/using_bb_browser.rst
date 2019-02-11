
.. _bb-browser-front-end:

BuildBarn Browser front-end
===========================

`BuildBarn Browser`_ is a *“simple web service written in Go that can display
objects stored in a Content Addressable Storage (CAS) and an Action Cache (AC)
that are used by the remote execution API.”* (REAPI). It originates from the
`Bazel Buildbarn project`_ but has support for any REAPI v2 compatible CAS and
AC backend and can thus be used together with BuildGrid in order to display web
reports for build action execution.

.. _BuildBarn Browser: https://github.com/buildbarn/bb-browser
.. _Bazel Buildbarn project: https://github.com/EdSchouten/bazel-buildbarn


.. _bb-browser-configuration:

Configuration
-------------


BuildBarn Browser
~~~~~~~~~~~~~~~~~

BuildBarn Browser uses a custom `BlobstoreConfiguration`_ protobuf message
stored in `text format`_ as a regular file on disk for configuration. This
message allows specifying theCAS and AC remote end-points to be used as data
backend. Setup is described on the `bb-browser repository homepage`_.

As an example, in order to access data from a CAS and AC remote execution server
available at ``storage.grid.build`` on port ``50052`` with TLS encryption and
client auth. credential forwarding, your ``blobstore.conf`` should be like:

.. code-block:: protobuf

   content_addressable_storage {
     grpc_metadata_forwarding {
       grpc {
         endpoint: "storage.grid.build:50052"
         tls: {}
       }
     }
   }

   action_cache {
     grpc_metadata_forwarding {
       grpc {
         endpoint: "storage.grid.build:50052"
         tls: {}
       }
     }
   }

.. _BlobstoreConfiguration: https://github.com/buildbarn/bb-storage/blob/master/pkg/proto/blobstore/blobstore.proto
.. _text format: https://developers.google.com/protocol-buffers/docs/reference/cpp/google.protobuf.text_format
.. _bb-browser repository homepage: https://github.com/buildbarn/bb-browser#setting-up-buildbarn-browser


BuildGrid
~~~~~~~~~

BuildGrid can be configured to generate URLs compatible with the BuildBarn
Browser service. They get generated once a build job completes an are returned
to the client along with the build results. It is then up to the client to
present it to the user or not. :ref:`RECC <recc-client>`, for example, will
print them on build failures.

In order to have BuildGrid to generate URL for a BuildBarn Browser service
available at ``https://browser.grid.build`` on port ``8080``, your BuildGrid
configuration should contain an ``execute`` service instance with:

.. code-block:: yaml

   instances:
     - name: local

       services:
         - !execution
           storage: *data-store
           action-cache: *build-cache
           action-browser-url: https://browser.grid.build:8080
