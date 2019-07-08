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

Recently we merged support for making BuildGrid "bounceable" by storing its
state in a configurable external SQL database (anything that SQLAlchemy supports
**should** be supported, and it has been shown to work with PostgreSQL and
SQLite).

Some work on improving performance by making bots long-poll instead of
rapidly polling over and over was also done recently, along with support for
outputting metrics in statsd format.

Next, we're looking at making it possible to horizontally scale BuildGrid
deployments, by making use of the external database support recently added to
share state between an arbitrary number of ExecutionService and BotsService
instances.

We're also working on implementing an indexed CAS server to faciliate a faster
FindMissingBlobs() and CAS cleanup. See
https://gitlab.com/BuildGrid/buildgrid/issues/181
for more details.


.. _getting-started:

Getting started
---------------

Please refer to the `documentation`_ for `installation`_ and `usage`_
instructions.

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
