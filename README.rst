.. _about:

About
=====


.. _what-is-it:

What is BuildGrid?
------------------

BuildGrid is a Python remote execution service which implements Google's
`Remote Execution API`_ and the `Remote Workers API`_. The project's goal is to
be able to execute build jobs remotely on a grid of computers in order to
massively speed up build times. Workers on the grid should be able to run with
different environments. It works with clients such as `Bazel`_,
`BuildStream`_ and `RECC`_, and is designed to be able to work with any client
that conforms to the above API protocols.

.. _Remote Execution API: https://github.com/bazelbuild/remote-apis
.. _Remote Workers API: https://docs.google.com/document/d/1s_AzRRD2mdyktKUj2HWBn99rMg_3tcPvdjx3MPbFidU/edit#heading=h.1u2taqr2h940
.. _BuildStream: https://wiki.gnome.org/Projects/BuildStream
.. _Bazel: https://bazel.build
.. _RECC: https://gitlab.com/bloomberg/recc


.. _whats-going-on:

What's Going On?
----------------

Recently BuildGrid's scheduler was made fully stateless (at least in terms of
state that needs to be persisted), with the option to run totally in-memory or
use an external database for all storage of persistent state. This gets us most
of the way to being able to horizontally scale BuildGrid.

We've also made numerous small improvements and bugfixes, and modified the
platform property matching logic to be consistent with the spec.

Next, we're finishing implementing the ability to horizontally scale BuildGrid
deployments, by removing the internal communication between the ExecutionService
and BotsService, which solves the problem of needing peers and bots for a
specific job to all be connected to the same server.

We're also working on implementing an indexed CAS server to faciliate a faster
FindMissingBlobs() and CAS cleanup. See
https://gitlab.com/BuildGrid/buildgrid/issues/181
for more details.


.. _getting-started:

Getting started
---------------

Please refer to the `documentation`_ for `installation`_ and `usage`_
instructions, plus guidelines for `contributing`_ to the project.

.. _contributing: https://buildgrid.gitlab.io/buildgrid/contributing.html
.. _documentation: https://buildgrid.gitlab.io/buildgrid
.. _installation: https://buildgrid.gitlab.io/buildgrid/installation.html
.. _usage: https://buildgrid.gitlab.io/buildgrid/using.html


.. _about-resources:

Resources
---------

- `Homepage`_
- `GitLab repository`_
- `Bug tracking`_
- `Mailing list`_
- `Slack channel`_ [`invite link`_]
- `FAQ`_

.. _Homepage: https://buildgrid.build
.. _GitLab repository: https://gitlab.com/BuildGrid/buildgrid
.. _Bug tracking: https://gitlab.com/BuildGrid/buildgrid/boards
.. _Mailing list: https://lists.buildgrid.build/cgi-bin/mailman/listinfo/buildgrid
.. _Slack channel: https://buildteamworld.slack.com/messages/CC9MKC203
.. _invite link: https://join.slack.com/t/buildteamworld/shared_invite/enQtMzkxNzE0MDMyMDY1LTRmZmM1OWE0OTFkMGE1YjU5Njc4ODEzYjc0MGMyOTM5ZTQ5MmE2YTQ1MzQwZDc5MWNhODY1ZmRkZTE4YjFhNjU
.. _FAQ: https://buildgrid.gitlab.io/buildgrid/faq.html
