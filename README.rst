BuildGrid
=========

BuildGrid is a python remote execution service which implements the `Remote Execution API <https://github.com/bazelbuild/remote-apis//>`_ and the `Remote Workers API <https://docs.google.com/document/d/1s_AzRRD2mdyktKUj2HWBn99rMg_3tcPvdjx3MPbFidU/edit#heading=h.1u2taqr2h940/>`_.

The goal is to be able to execute build jobs remotely on a grid of computers to massively speed up build times. Workers on the system will also be able to run with different environments. It is designed to work with but not exclusively with `BuildStream <https://wiki.gnome.org/Projects/BuildStream/>`_.

Install
-------

To install::

   git clone https://gitlab.com/BuildGrid/buildgrid.git
   cd buildgrid
   pip3 install --user -e .

This will install BuildGrid's python dependencies into your user’s homedir in ~/.local
and will run BuildGrid directly from the git checkout. It is recommended you adjust
your path with::

  export PATH="${PATH}:${HOME}/.local/bin"

Which you can add to the end of your `~/.bashrc`.

Instructions for a Dummy Work
----------------------

In one terminal, start a server::

  bgd server start

In another terminal, send a request for work::

  bgd execute request

The stage should show as `QUEUED` as it awaits a bot to pick up the work::

  bgd execute list

Create a bot session::

  bgd bot dummy

Show the work as completed::

  bgd execute list
