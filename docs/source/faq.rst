.. _faq:

FAQ
===

Q. Which clients are currently supported?
R. Currently we support `BuildStream`_, `Bazel`_ and `RECC`_. Though any client which implements v2 of the `Remote Execution API`_ should be supported.

Q. Where can I get the most up to date information?
R. We have a `mailing list`_ you can sign up to and a `slack channel`_ you can join.

Q. Help! This feature isn't working!?!
R. Please `raise`_ an issue or ask us on the `slack channel`_ for some help.

Q. Can I have feature XYZ? I've made a cool thing, can I have it in BuildGrid?
R. Check that a similar issue hasn't been raised already and then `raise`_ one yourself if it hasn't. From there we can discuss the feature. We would encourage you to contact the mailing list if you wish to implement a large feature so everyone aware of your idea. Then, create a `merge request`_. The worst we can do is politely decline your work, otherwise you're now officially a contributor! Have a quick read of the `contributing`_ guidelines before making your patch submission.

Q. How do I install BuildGrid?
R. Check out this `installation page`_.

Q. How do I check on the status of a job?
R. Using the command line tool you can simply list all the current jobs and select one for the status

    .. code-block:: sh

       bgd operation list
       bgd operatation status <insert-operation-id>

Q. How does CAS expiry work?
R. We are working hard on automatic CAS expiry but until we have that, we recommend you regularly clean the CAS folder yourself.

Q. What's the difference between a bot and a worker?
R. A worker can be thought of but isn't limited to a physical machine. For example, it can be a collection of physical devices, docker images or a selected number of CPUs. The worker runs what we call a bot. This is a program that controls the worker; giving it actions to do, returning results to the central server and monitoring the status of the worker. The central server can communicate with a collection of bots via the `BotsInterface`.

Q. Where can I find more information about the API?
R. We recommend you read the proto files for the `Remote Execution API`_ and `Remote Workers API`_. There is a more `detailed document for the Remote Execution API`_ and another `detailed document for the Remote Workers API`_.

Q. What's going on in the _proto folder?
R. This project uses gRPC and protocol buffers for network messaging. The .proto files are used to generate python modules which are then used in the project. See the `grpc guide`_ for more details.

Q. How do I set up a BuildGrid?
R. You can follow this guide to give you an understanding on basic usecases: :ref:`using`. There is also additional information on how to configure your Grid here: :ref:`server-configuration`.


.. _BuildStream: https://buildstream.build
.. _Bazel: https://bazel.build/
.. _RECC: https://gitlab.com/bloomberg/recc
.. _Remote Execution API: https://gitlab.com/BuildGrid/buildgrid/blob/master/buildgrid/_protos/build/bazel/remote/execution/v2/remote_execution.proto
.. _Remote Workers API: https://gitlab.com/BuildGrid/buildgrid/tree/master/buildgrid/_protos/google/devtools/remoteworkers/v1test2
.. _mailing list: https://lists.buildgrid.build/cgi-bin/mailman/listinfo/buildgrid
.. _slack channel: https://join.slack.com/t/buildteamworld/shared_invite/enQtMzkxNzE0MDMyMDY1LTRmZmM1OWE0OTFkMGE1YjU5Njc4ODEzYjc0MGMyOTM5ZTQ5MmE2YTQ1MzQwZDc5MWNhODY1ZmRkZTE4YjFhNjU
.. _merge request: https://gitlab.com/BuildGrid/buildgrid/merge_requests
.. _contributing: https://gitlab.com/BuildGrid/buildgrid/blob/master/CONTRIBUTING.rst
.. _raise: https://gitlab.com/BuildGrid/buildgrid/issues
.. _installation page: https://buildgrid.gitlab.io/buildgrid/installation.html
.. _detailed document for the Remote Execution API: https://docs.google.com/document/d/1AaGk7fOPByEvpAbqeXIyE8HX_A3_axxNnvroblTZ_6s/
.. _detailed document for the Remote Workers API: https://docs.google.com/document/d/1s_AzRRD2mdyktKUj2HWBn99rMg_3tcPvdjx3MPbFidU/
.. _grpc guide: https://grpc.io/docs/guides/
