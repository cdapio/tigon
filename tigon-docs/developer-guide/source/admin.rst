.. :author: Cask Data, Inc.
   :description: Administration of Tigon Applications
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Administration
============================================

.. highlight:: console

Standalone Mode
===============

To run Tigon in standalone mode::

  $ run_standalone.sh <path-to-flow-jar> <flow-class-name> <run-time-args>
  
Example::

  $ run_standalone.sh DataParser.jar com.example.TopFlow --key1=value1 --key2=value2
  
The Flow program will be started and run in the same Java process. Only one Flow can be
run at a time, and the number of Flowlet instances cannot be changed once started.
Runtime arguments are dependent on the application being run.

To stop the application, its Flow and Flowlets, quit the shell script using ``control-C``.


Distributed Mode
================

To run Tigon in distributed mode::

  $ run_distributed.sh <zookeeper-quorum> <hdfs-namespace>
  
Example::

  $ run_distributed.sh example.com:2181/quorum tigon

The distributed mode will start the Flow. Flowlet instances will be spun up as YARN
containers in the Hadoop cluster.

The Flow will appear as a YARN Application and will be visible through
the YARN UI, located at ``http://<master-node>:8088``. For the example above, that would be::

  http://example.com:8088

The distributed mode has a command-line interface that can be used to control the Flow and
its lifecycle. A complete list of available commands is documented in the `Tigon Tools <tools.html>`__;
to list the running Flows, you can use::

  tigon > list

You can list all the running Flows, start and stop a Flow, check the status of a running Flow,
and delete all queues associated with a particlar Flow.

Note that, in distributed mode, Flows continue to run even after you quit the command-line
interface unless you have explicitly stopped them. You can stop and delete a particluar Flow 
with the command::

  tigon > delete <flow-name>

You can then exit the command-line interface either with ``exit`` or ``Control-C``.


Where to Go Next
================

Now that you're familiar with how to run Tigon, take a look at:

- :doc:`licenses`, which covers how Tigon has been released.
