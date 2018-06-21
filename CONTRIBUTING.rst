Contributing to BuildGrid
=========================
Some guidlines for people wanting to contribute.

Feature Additions
-----------------

There are currently no mailing lists for BuildGrid - we are working on this, though. Any major feature additions should be raised as an
issue here on gitlab and it is recommended they are proposed in advance of commencing work.

The author of any feature should take ownership and is expected to support it for a reasonable
timeframe. This means addressing any unforseen side effects and quirks the feature may have introduced.

Patch Submissions
-----------------

We will be running `trunk based development <https://trunkbaseddevelopment.com>`_. The idea behind this is that merge requests to the trunk will be small and made often making the review process quicker. If possible,
it is preferred that merge requests address specific issues.

Branches must be submitted as merge requests on gitlab and should be associated with an issue report on
gitlab. Each commit should address a specific issue number in the commit message.

Merge requests that are not yet ready for review must be prefixed with the `WIP:` identifier.

Sbmitted branches should not contain a history of work done.

Unit tests should be a separate commit.

Commit messages
~~~~~~~~~~~~~~~
Commit messages must be formatted with a brief summary line, optionally followed by an empty line and then a
free form detailed description of the change.

The summary line must start with what changed, followed by a colon and a very brief description of the
change.

If there is an associated issue, it **must** be mentioned somewhere in the commit message.

**Example**::

  worker.py: Fixed to be more human than human

  Gifted the worker with a past so we can create
  a cushion or a pillow for their emotions and
  consequently, we can control them better.
  
  This fixes issue #8

  
For more tips, please see `this <https://chris.beams.io/posts/git-commit/#seven-rules/>`_ article.

Coding style
------------
Coding style details for BuildGrid.


Style guide
~~~~~~~~~~~
Python coding style for BuildGrid is pep8, which is documented here: https://www.python.org/dev/peps/pep-0008/

We have a couple of minor exceptions to this standard, we dont want to compromise
code readability by being overly restrictive on line length for instance.


Imports
~~~~~~~
Module imports inside BuildGrid are done with relative ``.`` notation

Good::

  from .worker import Worker

Bad::

  from buildgrid.worker import Worker

Ordering
''''''''
For better readability and consistency, we try to keep private symbols below
public symbols. In the case of public modules where we may have a mix of
*API private* and *local private* symbols, *API private* symbols should come
before *local private* symbols.


Symbol naming
'''''''''''''
Any private symbol must start with a single leading underscore for two reasons:

* So that it does not bleed into documentation and *public API*.

* So that it is clear to developers which symbols are not used outside of the declaring *scope*

Remember that with python, the modules (python files) are also symbols
within their containing *package*, as such; modules which are entirely
private to BuildGrid are named as such, e.g. ``_roy.py``.
