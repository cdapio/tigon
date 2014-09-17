.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Introduction
============================================

Tigon is a high-speed stream database. Tigon's original purpose was network monitoring, though
there are many other applications that can take advantage of its features:

- Real-time Stream processing engine;
- Ability to handle extremely large data flows;
- Data is touched only once, useless data is not gathered, and results are quickly produced;
- Runs collections of queries using pipelined query plans and a SQL-like language;
- Supports a significant amount of parallelization; and 
- Able to transparently handle complex record routing in large parallelized implementations;
- Design to work with Apache Hadoop |(TM)|;
- All software available as open-source;

Getting Started
===============

Prerequisites
-------------

Tigon is supported on \*NIX systems such as Linux and Macintosh OS X.
It is not supported on Microsoft Windows.

To install and use Tigon and its included examples, there are a few prerequisites:

1. JDK 6 or JDK 7 (required to run Tigon; note that $JAVA_HOME should be set)
#. GCC
#. G++
#. libcurl (if it is not already included in your operating system)
#. Apache Maven 3.0+ (required to build the example applications)


Download
--------

Pre-compiled sources and related files can be downloaded in a zip file: 
`tigon-0.1.0.zip <http://cask.co/downloads/tigon/tigon-0.1.0.zip>`.


Install 
-------

Once the download has completed, unzip the file in a suitable location.

  
Building Tigon from Source
--------------------------

.. highlight:: console

You can also get started with Tigon by building directly from the latest source code::

  git clone https://github.com/cask/tigon.git
  cd tigon
  mvn clean package

After the build completes, you will have a distribution of Tigon under the
`tigon-distribution/target/` directory.  

Take the `tigon-<version>.tar.gz` file and unzip it into a suitable location.

For more build options, please refer to the [build instructions](BUILD.md) included in the
source distribution.


Configuration
-------------

Macintosh OS X
..............

.. highlight:: console

Tigon will not run with the default shared memory settings of the Macintosh, as 
OS X does not provide sufficient shared memory regions by default. 
To run Tigon, you'll need to revise these settings by executing these commands::

  sudo sysctl -w kern.sysv.shmall=102400

  sudo sysctl -w kern.sysv.shmseg=100

  sudo sysctl -w kern.sysv.shmmax=419430400


Setting up Your Development Environment
---------------------------------------


Quick Start
-----------

Visit our web site for a [Quick Start](http://docs.cask.co/docs/tigon/current/quickstart.html)
that will guide you through installing Tigon, running an example that....  



Where to Go Next
================

Now that you've had an introduction to Tigon, take a look at:

- `Concepts <concepts.html>`__, which covers the basic concepts behind Tigon.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :trim: