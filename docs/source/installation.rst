
.. _installation:

Installation
============

How to install BuildGrid onto your machine.

To install:

.. code-block:: sh

   git clone https://gitlab.com/BuildGrid/buildgrid.git
   cd buildgrid
   pip3 install --user -e .

This will install BuildGrid's python dependencies into your user’s homedir in ~/.local
and will run BuildGrid directly from the git checkout. It is recommended you adjust
your path with:

.. code-block:: sh

   export PATH="${PATH}:${HOME}/.local/bin"

Which you can add to the end of your `~/.bashrc`.
