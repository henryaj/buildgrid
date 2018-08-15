BuildGrid
=========

.. image:: https://gitlab.com/Buildgrid/buildgrid/badges/master/pipeline.svg
   :target: https://gitlab.com/BuildStream/buildstream/commits/master

.. image:: https://gitlab.com/BuildGrid/buildgrid/badges/master/coverage.svg?job=coverage
   :target: https://gitlab.com/BuildGrid/buildgrid/commits/master


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

  bgd execute request-dummy

The stage should show as `QUEUED` as it awaits a bot to pick up the work::

  bgd execute list

Create a bot session::

  bgd bot dummy

Show the work as completed::

  bgd execute list

Instructions for a Simple Build
-------------------------------

This example covers a simple build. The user will upload a directory containing a C file and a command to the CAS. The bot will then fetch the uploaded directory and command which will then be run inside a temporary directory. The result will then be uploaded to the CAS and downloaded by the user. This is an early demo and still lacks a few features such as symlink support and checking to see if files exist in the CAS before executing a command.

Create a new directory called `test-buildgrid/` and place the following C file in it called `hello.c`::

  #include <stdio.h>
  int main()
  {
   printf("Hello, World!\n");
   return 0;
  }

Now start a BuildGrid server, passing it a directory it can write a CAS to::

  bgd server start --cas disk --cas-cache disk --cas-disk-directory /path/to/empty/directory

Start the following bot session::

  bgd bot temp-directory

Upload the directory containing the C file::

  bgd cas upload-dir /path/to/test-buildgrid

Now we send an execution request to the bot with the name of the epxected `output-file`, a boolean describing if it is executeable, the path to the directory we uploaded in order to calculate the digest and finally the command to run on the bot::

  bgd execute command --output-file hello True /path/to/test-buildgrid -- gcc -Wall hello.c -o hello

The resulting executeable should have returned to a new directory called `testing/`
