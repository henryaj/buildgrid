
.. _simple-build:

Simple build
============

This example covers a simple build. The user will upload a directory containing
a C file and a command to the CAS. The bot will then fetch the uploaded
directory and command which will then be run inside a temporary directory. The
result will then be uploaded to the CAS and downloaded by the user. This is an
early demo and still lacks a few features such as symlink support and checking
to see if files exist in the CAS before executing a command.

Create a new directory called `test-buildgrid/` and place the following C file
in it called `hello.c`:

.. code-block:: C

   #include <stdio.h>
   int main()
   {
     printf("Hello, World!\n");
     return 0;
   }

Now start a BuildGrid server, passing it a directory it can write a CAS to:

.. code-block:: sh

   bgd server start --cas disk --cas-cache disk --cas-disk-directory /path/to/empty/directory

Start the following bot session:

.. code-block:: sh

   bgd bot temp-directory

Upload the directory containing the C file:

.. code-block:: sh

   bgd cas upload-dir /path/to/test-buildgrid

Now we send an execution request to the bot with the name of the epxected
``output-file``, a boolean describing if it is executeable, the path to the
directory we uploaded in order to calculate the digest and finally the command
to run on the bot:

.. code-block:: sh

   bgd execute command --output-file hello True /path/to/test-buildgrid -- gcc -Wall hello.c -o hello

The resulting executeable should have returned to a new directory called
``testing``.
