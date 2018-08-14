Contributing to BuildGrid
=========================
Some guidelines for people wanting to contribute. Also please always feel free to speak to us, we're very friendly :-)

Feature Additions
-----------------

We welcome contributions in the form of bug fixes or feature additions / enhancements. Please discuss with us before submitting anything, as we may well have some important context which will could help guide your efforts.  

Any major feature additions should be raised as a proposal on the `Mailing List <https://lists.buildgrid.build/cgi-bin/mailman/listinfo/buildgrid/>`_ to be discussed, and then eventually followed up with an issue here on gitlab. We recommend that you propose the feature in advance of commencing work. We are also on irc - you can find us on #buildgrid on freenode.

The author of any patch is expected to take ownership of that code and is to support it for a reasonable time-frame. This means addressing any unforeseen side effects and quirks the feature may have introduced. More on this below in 'Granting Committer Access'.

Patch Submissions
-----------------

We will be running `trunk based development <https://trunkbaseddevelopment.com>`_. The idea behind this is that merge requests to the trunk will be small and made often, thus making the review and merge process as fast as possible. We do not want to end up with a huge backlog of outstanding merge requests. If possible,
it is preferred that merge requests address specific points and clearly outline what problem they are solving.

Branches must be submitted as merge requests on gitlab and should be associated with an issue report on gitlab, whenever possible. If it's a small change, we'll accept an MR without it being associated to a gitlab issue, but generally we prefer an issue to be raised in advance. This is so that we can track the work that is currently in progress on the project - please see our Gitlab policy below.

Each commit should address a specific gitlab issue number in the commit message. This is really important for provenance reasons.

Merge requests that are not yet ready for review must be prefixed with the `WIP:` identifier, but if we stick to trunk based development then the 'WIP:' identifier will not stay around for very long on a merge request.

When a merge request is ready for review, please find someone willing to do the review (ideally a maintainer) and assign them the MR on gitlab, leaving a comment asking for their review. 

Submitted branches should not contain a history of work done.

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

Granting Committer Access
-------------------------

We'll hand out commit access to anyone who has successfully landed a single patch to the code base. Please request this via irc or the mailing list.

This of course relies on contributors being responsive and show willingness to address problems after landing branches there should not be any problems here.

What we are expecting of committers here in general is basically to
escalate the review in cases of uncertainty:

* If the patch/branch is very trivial (obvious few line changes or typos etc), and you are confident of the change, there is no need for review.

* If the patch/branch is non trivial, please obtain a review from another committer who is familiar with the area which the branch effects. An approval from someone who is not the patch author will be needed before any merge. 

We don't have any detailed policy for "bad actors", but will of course handle things on a case by case basis - commit access should not result in commit wars or be used as a tool to subvert the project when disagreements arise, such incidents (if any) would surely lead to temporary suspension of commit rights.

BuildGrid policy for use of Gitlab features
-------------------------------------------

We intend to make use of some of gitlab's features in order to structure the activity of the BuildGrid project. In doing so we are trying to achieve the following goals:

* Full transparency of the current WIP items 
* Provide a view of all current and planned activity which is relatively easy for the viewer to digest
* Ensure that we keep it simple and easy to contribute to the project

We propose to make use of the following Gitlab features:

* Milestones
* Labels
* Boards
* Templates

Milestones
~~~~~~~~~~
`Milestones <https://docs.gitlab.com/ee/user/project/milestones/>`_ are based on periods of time and what we want to achieve within those periods of time.

We have seen them used in the same way as `Epics <https://docs.gitlab.com/ee/user/group/epics/index.html#doc-nav/>`_ in other projects (since the Epic feature is only available with GitLab Ultimate) and this does not work. Milestones must be time-line based.

Milestones can overlap, and we can be working towards multiple milestones at any one time. They allow us to group together all sub tasks into an overall aim.

Labels
~~~~~~
`Labels <https://docs.gitlab.com/ee/user/project/labels.html/>`_ allow us to filter tickets on gitlab in useful ways. They do complexity and effort as they grow in number and usage, though, so the general approach is to have the minimum possible.

Type Labels
'''''''''''
We have:

* Bug
* Documentation
* Enhancement
* Tests

This is useful for filtering different types of issues. We may expand this at some point.

Priority Labels
'''''''''''''''
For now, we only have 'High Priority', which indicates an urgent task. We may add more granularity if we get more contributors. 

Status
'''''
We have:

* ToDo
* Doing

These labels are used when structuring tickets on a Board. GitLab issues start life in the 'Backlog' column by default, and we move them into 'ToDo' when they are coming up in the next few weeks. 'Doing' is only for when an item is currently being worked on. These labels don't have to be manually applied, they are applied by GitLab when moving the issue from column to column when using a Board - see below.

Issue Boards
~~~~~~~~~~~~
`Boards <https://docs.gitlab.com/ee/user/project/issue_board.html#doc-nav/>`_ allow you to visualise and manage issues in a simple way, and we can create different types of board by filtering labels. For now, we are just utilising Boards in order to be able to see all of the currently in flight items at a glance.

Templates
~~~~~~~~~
`Issue templates <https://docs.gitlab.com/ee/user/project/description_templates.html#doc-nav/>`_ help us to receive good quality information in issues.

