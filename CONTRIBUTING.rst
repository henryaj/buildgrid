
.. _contributing:

Contributing
============

Some guidelines for people wanting to contribute. Also please always feel free
to speak to us, we're very friendly :-)


.. _feature-additions:

Feature additions
-----------------

We welcome contributions in the form of bug fixes or feature additions. Please
discuss with us before submitting anything, as we may well have some important
context which will could help guide your efforts.

Any major feature additions should be raised first as a proposal on the
`BuildGrid mailing list`_ to be discussed, and then eventually followed up with
an issue on GitLab. We recommend that you propose the feature in advance of
commencing work.

The author of any patch is expected to take ownership of that code and is to
support it for a reasonable time-frame. This means addressing any unforeseen
side effects and quirks the feature may have introduced. More on this below in
:ref:`Committer access <committer-access>`.

.. _BuildGrid mailing list: https://lists.buildgrid.build/cgi-bin/mailman/listinfo/buildgrid


.. _patch-submissions:

Patch submissions
-----------------

We are running `trunk based development`_. The idea behind this is that merge
requests to the trunk will be small and made often, thus making the review and
merge process as fast as possible. We do not want to end up with a huge backlog
of outstanding merge requests. If possible, it is preferred that merge requests
address specific points and clearly outline what problem they are solving.

Branches must be submitted as merge requests (MR) on GitLab and should be
associated with an issue, whenever possible. If it's a small change, we'll
accept an MR without it being associated to an issue, but generally we prefer an
issue to be raised in advance. This is so that we can track the work that is
currently in progress on the project.

Below is a list of good patch submission good practice:

- Each commit should address a specific issue number in the commit message. This
  is really important for provenance reasons.
- Merge requests that are not yet ready for review must be prefixed with the
  ``WIP:`` identifier, but if we stick to trunk based development then the
  ``WIP:`` identifier will not stay around for very long on a merge request.
- When a merge request is ready for review, please find someone willing to do
  the review (ideally a maintainer) and assign them the MR, leaving a comment
  asking for their review.
- Submitted branches should not contain a history of work done.
- Unit tests should be a separate commit.

.. _trunk based development: https://trunkbaseddevelopment.com


Commit messages
~~~~~~~~~~~~~~~

Commit messages must be formatted with a brief summary line, optionally followed
by an empty line and then a free form detailed description of the change. The
summary line must start with what changed, followed by a colon and a very brief
description of the change. If there is an associated issue, it **must** be
mentioned somewhere in the commit message.

**Example**::

   worker.py: Fixed to be more human than human

   Gifted the worker with a past so we can create
   a cushion or a pillow for their emotions and
   consequently, we can control them better.

   This fixes issue #8.

For more tips, please read `The seven rules of a great Git commit message`_.

.. _The seven rules of a great Git commit message: https://chris.beams.io/posts/git-commit/#seven-rules


.. _coding-style:

Coding style
------------

Python coding style for BuildGrid is `PEP 8`_. We do have a couple of minor
exceptions to this standard, we dont want to compromise code readability by
being overly restrictive on line length for instance.

BuildGrid's test suite includes a PEP8 style compliance check phase (using
`pep8`_) and a code linting phase (using `pylint`_). That test suite is
automatically run for every change submitted to the GitLab server and the merge
request sytem requires the test suite execution to succed before changes can
be pulled upstream. This means you have to respect the BuildGrid coding style.

Configuration and exceptions for ``pep8`` and ``pylint`` can be found in:

- The `setup.cfg`_ file for ``pep8``.
- The `.pylintrc`_ file for ``pylint``.

.. _PEP 8: https://www.python.org/dev/peps/pep-0008
.. _pep8: https://pep8.readthedocs.io
.. _pylint: https://pylint.readthedocs.io
.. _setup.cfg: https://gitlab.com/BuildGrid/buildgrid/blob/master/setup.cfg
.. _.pylintrc: https://gitlab.com/BuildGrid/buildgrid/blob/master/.pylintrc


Imports
~~~~~~~

Module imports inside BuildGrid are done with relative ``.`` notation

Good::

  from .worker import Worker

Bad::

  from buildgrid.worker import Worker


Symbol naming
'''''''''''''

Any private symbol must start with a single leading underscore for two reasons:

- So that it does not bleed into documentation and *public API*.
- So that it is clear to developers which symbols are not used outside of the
  declaring *scope*.

Remember that with python, the modules (python files) are also symbols within
their containing *package*, as such; modules which are entirely private to
BuildGrid are named as such, e.g. ``_roy.py``.


.. _codebase-testing:

Testing
-------

BuildGrid is using `pytest`_ for regression and newly added code testing. The
test suite contains a serie of unit-tests and also run linting tools in order to
detect coding-style_ breakage. The full test suite is automatically executed by
GitLab CI system for every push to the server. Passing all the tests is a
mandatory requirement for any merge request to the trunk.

.. _pytest: https://docs.pytest.org


Running tests
~~~~~~~~~~~~~

In order to run the entire test suite, simply run:

.. code-block:: sh

   python3 setup.py test

You can use the ``--addopt`` function to feed arguments to pytest. For example,
if you want to see the ``stdout`` and ``stderr`` generated y the test, run:

.. code-block:: sh

   python3 setup.py test  --addopts -s

If you want run a  specific test instead of the entire suite use:

.. code-block:: sh

   python3 setup.py test  --addopts tests/cas/test_client

pyest's `usage documentation section`_ details the different command line
options that can be used when invoking the test runner.

.. _usage documentation section: https://docs.pytest.org/en/latest/usage.html


Test coverage
~~~~~~~~~~~~~

We are doing our best at keeping BuildGrid's test coverage score as high as
possible. Doing so, we ask for any merge request to include necessary test
additions and/or modifications in order to maintain that coverage level. A
detailed `coverage report`_ is produced and publish for any change merged to the
trunk.

.. _coverage report: https://buildgrid.gitlab.io/buildgrid/coverage/


.. _committer-access:

Committer access
----------------

We'll hand out commit access to anyone who has successfully landed a single
patch to the code base. Please request this via Slack or the mailing list.

This of course relies on contributors being responsive and show willingness to
address problems after landing branches there should not be any problems here.

What we are expecting of committers here in general is basically to escalate the
review in cases of uncertainty:

- If the change is very trivial (obvious few line changes, typos…), and you are
  confident of the change, there is no need for review.
- If the change is non trivial, please obtain a review from another committer
  who is familiar with the area which the branch effects. An approval from
  someone who is not the patch author will be needed before any merge.

.. note::

   We don't have any detailed policy for "bad actors", but will of course handle
   things on a case by case basis - commit access should not result in commit
   wars or be used as a tool to subvert the project when disagreements arise.
   Such incidents (if any) would surely lead to temporary suspension of commit
   rights.


.. _gitlab-features:

GitLab features
---------------

We intend to make use of some of GitLab's features in order to structure the
activity of the BuildGrid project. In doing so we are trying to achieve the
following goals:

- Full transparency of the current work in progress items.
- Provide a view of all current and planned activity which is relatively easy
  for the viewer to digest.
- Ensure that we keep it simple and easy to contribute to the project.

We are currenlty using the following GitLab features:

- `Milestones`_: we have seen them used in the same way as `Epics`_ in other
  projects. BuildGrid milestones must be time-line based, can overlap and we can
  be working towards multiple milestones at any one time. They allow us to group
  together all sub tasks into an overall aim. See our `BuildGrid milestones`_.
- `Labels`_: allow us to filter tickets in useful ways. They do complexity and
  effort as they grow in number and usage, though, so the general approach is
  to have the minimum possible. See our `BuildGrid labels`_.
- `Boards`_: allow us to visualise and manage issues and labels in a simple way.
  For now, we are only utilising one boards. Issues start life in the
  ``Backlog`` column by default, and we move them into ``ToDo`` when they are
  coming up in the next few weeks. ``Doing`` is only for when an item is
  currently being worked on. Moving an issue from column to column automatically
  adjust the tagged labels. See our `BuildGrid boards`_.

.. _Milestones: https://docs.gitlab.com/ee/user/project/milestones
.. _Epics: https://docs.gitlab.com/ee/user/group/epics
.. _BuildGrid milestones: https://gitlab.com/BuildGrid/buildgrid/milestones
.. _Labels: https://docs.gitlab.com/ee/user/project/labels.html
.. _BuildGrid labels: https://gitlab.com/BuildGrid/buildgrid/labels
.. _Boards: https://docs.gitlab.com/ee/user/project/issue_board.html
.. _BuildGrid boards: https://gitlab.com/BuildGrid/buildgrid/boards
.. _Templates: https://docs.gitlab.com/ee/user/project/description_templates.html
