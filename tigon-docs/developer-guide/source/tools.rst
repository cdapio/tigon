.. :author: Cask Data, Inc.
   :description: Command-line interface
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Tools
============================================

Tigon Command-Line Interface
============================

Introduction
------------

The Command-Line Interface (CLI) provides methods to interact with Tigon from within a shell,
similar to HBase shell or ``bash``. It is located within the SDK, at ``bin/tigon-cli`` as a Bash
script file. It is also packaged in the SDK as a JAR file, at ``bin/tigon-cli.jar``.

Usage
-----

The CLI may be used in two ways: interactive mode and non-interactive mode.

Interactive Mode
----------------

.. highlight:: console

To run the CLI in interactive mode, run the ``tigon-cli`` executable with no arguments from the terminal::

  $ /bin/tigon-cli

The executable should bring you into a shell, with this prompt::

  tigon (localhost:10000)>

This indicates that the CLI is currently set to interact with the Tigon server at ``localhost``.
There are two ways to interact with a different Tigon server:

- To interact with a different Tigon server by default, set the environment variable ``TIGON_HOST`` to a hostname.
- To change the current Tigon server, run the command ``connect example.com``.

For example, with ``TIGON_HOST`` set to ``example.com``, the Shell Client would be interacting with
a Tigon instance at ``example.com``, port ``10000``::

  tigon (example.com:10000)>

To list all of the available commands, enter ``help``::

  tigon (localhost:10000)> help

Non-Interactive Mode
--------------------

To run the CLI in non-interactive mode, run the ``tigon-cli`` executable, passing the command you want executed
as the argument. For example, to list all Flows currently running in Tigon, execute::

  tigon list

Available Commands
------------------

These are the available commands:

.. csv-table::
   :header: Command,Description
   :widths: 50, 50

   **General**
   ``help``,Prints this helper text
   ``version``,Prints the version
   ``status``,Prints the current Tigon system status
   ``exit``,Exits the shell

   **Starting Elements**
   ``start <path-to-jar> <flow-classname>``,Starts a Flow
   ``stop <flow-name>``,Stops a Flow *flow-name*
   ``delete <flow-name>``,Stops and Deletes the Queues for a Flow *flow-name*
   ``set <flow-name> <flowlet-name> <instances>``,Set the number of instance of a Flowlet *flowlet-name* for a Flow *flow-name*

   **Listing Elements**
   ``list``,Lists all Flows which are currently running
   ``serviceinfo <flow-name>``,Prints all Services announced in a Flow *flow-name*
   ``discover <flow-name> <service-name>``,Discovers a service endpoint of a Service *service-name* for a Flow *flow-name*
   ``flowletinfo <flow-name>``,Prints Flowlet Names and corresponding Instances for a Flow *flow-name*
   ``showlogs <flow-name>``,Shows live logs for a Flow *flow-name*


Where to Go Next
================

Now that you're seen the command line tool for Tigon, take a look at:

- `Admin <admin.html>`__, which describes administrating a Tigon application.

