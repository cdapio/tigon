.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Introduction
============================================

**Tigon** is an open-source, real-time, low-latency, high-throughput stream processing framework.

Tigon is a collaborative effort between Cask Data, Inc. and AT&T that combines 
technologies from these companies to create a disruptive new framework to handle a diverse
set of real-time streaming requirements.

Cask Data has built technology that provides scalable, reliable, and persistent high-throughput
event processing with high-level Java APIs using Hadoop and HBase.

AT&T has built a streaming engine that provides massively scalable, flexible, and in-memory
low-latency stream processing with a SQL-like query Language.

Together, they have combined to create **Tigon**.

There are many applications that can take advantage of these features:

- Ability to handle extremely large data flows;
- Exactly-once event processing using an app-level Java API with consistency, reliability, and persistence;
- Streaming database using a SQL-like language to filter, group and join data streams in-memory;
- Runs collections of queries using pipelined query plans;
- Able to transparently handle complex record routing in large parallelized implementations;
- Runs and scales as a native Apache Hadoop |(TM)| YARN Application;
- Reads, writes, and tightly integrates with HDFS and HBase;
- Supports a significant amount of parallelization;
- Fault-tolerance and horizontal scalability without burdening the developer;
- Enterprise security features with debugging, logging, and monitoring tools;
- Simpler programming model, tools and UI; and 
- Open-source software and development process.

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
#. libz
#. Apache Maven 3.0+ (required to build the example applications)
#. In standalone mode, both Perl 5.x and Python 3.x are required


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

Is It Building?
...............

============================================================================= ==================
 Build                                                                         Status / Version
============================================================================= ==================
`Travis Continuous Integration Build <https://travis-ci.org/caskco/tigon>`__   |travis-tigon|
`GitHub Version <https://github.com/caskco/tigon/releases/latest>`__           |github-tigon|
============================================================================= ==================

.. |travis-tigon| image:: https://travis-ci.org/caskco/tigon.svg?branch=develop
                  :height: 30px

.. |github-tigon| image:: http://img.shields.io/github/release/caskco/tigon.svg
                  :height: 30px

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

Visit our web site for a `Quick Start <http://docs.cask.co/docs/tigon/current/quickstart.html>`__
that will guide you through installing Tigon, running an example that....  



Where to Go Next
================

Now that you've had an introduction to Tigon, take a look at:

- `Concepts <concepts.html>`__, which covers the basic concepts behind Tigon.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim: