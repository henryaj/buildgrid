.. _notes_for_developers:

Notes for Developers
====================

.. _upgrading-protos:

Upgrading the gRPC protobuf files
----------------------------

Buildgrid's gRPC stubs are not built as part of installation. Instead, they are
precompiled and shipped with the source code. The protobufs are prone to
compatibility-breaking changes, so we update them manually.

First bring the updated proto file into the source tree. For example, if updating
the remote execution proto, replace the old
``buildgrid/_protos/build/bazel/remote/execution/v2/remote_execution.proto``
with the newer one.

Then, compile the protobufs. Continuing with the ``remote_execution.proto`` example...

    .. code-block:: python

        pip install grpcio grpcio-tools
        python -m grpc_tools.protoc -Ibuildgrid/_protos/ --python_out=build/bazel/remote/execution/v2/ --grpc_python_out=build/bazel/remote/execution/v2/ build/bazel/remote/execution/v2/remote_execution.proto

The most important thing here is to make sure that ``buildgrid/_protos`` is in your include path with ``-Ibuildgrid/_protos``.
If all goes well, the new files should have been generated.