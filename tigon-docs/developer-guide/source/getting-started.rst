.. :author: Cask Data, Inc.
   :description: Getting started guide
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Getting Started with Tigon
============================================

We recommend you follow these steps to get started with Tigon:

1. Check that you have the `prerequisites`_ and install if necessary.
#. `Download`_ the pre-compiled sources and related files.
#. `Configure`_ your system, if necessary.
#. Run the `examples`_ to test your installation and learn about Tigon.


Prerequisites
=============

Tigon is supported on \*NIX systems such as Linux and Macintosh OS X.
It is not supported on Microsoft Windows.

To install and use Tigon and its included examples, there are a few prerequisites:

1. JDK 6 or JDK 7 (required to run Tigon; note that $JAVA_HOME should be set)
#. GCC
#. G++
#. libz (if it is not already included in your operating system; see note following)
#. Apache Maven 3.0+ (required to build the example applications)

**Note:** You need the ``libz.so`` at runtime, which is provided by the ``libz`` package.
At compile time, you need the ``zlib.h`` header, provided by ``zlib-devel`` on RHEL
systems and by ``libz-dev`` on Debian-based systems. Users who provide ``libz`` via
compilation (download a ``libz`` tarball, ``./configure && make && make install``) will
have the header provided during the make install.

**Note:** To run the TigonSQL Stream Engine outside of Tigon, both Perl 5.x and Python 3.x
are required.


Download
========

Pre-compiled sources and related files can be downloaded in a zip file: 
`tigon-developer-release-0.2.0.zip. 
<http://repository.cask.co/downloads/co/cask/tigon/tigon-developer-release/0.2.0/tigon-developer-release-0.2.0.zip>`__


Install 
=======

Once the download has completed, unzip the file to a suitable location.


Creating an Application
=======================

.. highlight:: console

The best way to start developing a Tigon application is by using the Maven archetype::

  $ mvn archetype:generate \
    -DarchetypeGroupId=co.cask.tigon  \
    -DarchetypeArtifactId=tigon-app-archetype  
    -DarchetypeVersion=0.2.0

This creates a Maven project with all required dependencies, Maven plugins, and a simple
application template for the development of your application. You can import this Maven
project into your preferred IDE—such as Eclipse or IntelliJ—and start developing your
first Tigon application.


Building Tigon from Source
==========================

.. highlight:: console

You can also build Tigon directly from the latest source code::

  git clone https://github.com/caskdata/tigon.git
  cd tigon
  mvn clean package -DskipTests -Pdist

After the build completes, you will have a distribution of Tigon under the
`tigon-distribution/target/` directory.  

Take the `tigon-sdk-<version>.zip` file and unzip it into a suitable location.


Is It Building?
---------------
These links allow you to check the status of the automated builds of the source code:

- `Bamboo Build <https://builds.cask.co/browse/TIG>`__
- `GitHub Version <https://github.com/caskdata/tigon/releases/latest>`__           

.. _configure:

Configuration
=============

Macintosh OS X
--------------

.. highlight:: console

TigonSQL will not run with the default shared memory settings of the Macintosh, as 
OS X does not provide sufficient shared memory regions by default. 
To run TigonSQL, you'll need to revise these settings by executing these commands::

  sudo sysctl -w kern.sysv.shmall=102400

  sudo sysctl -w kern.sysv.shmseg=100

  sudo sysctl -w kern.sysv.shmmax=419430400


Problems with the Downloaded Package?
---------------------------------------

If the downloaded package doesn't work, try to build a tarball for your system
as described above.

If you face problems using TigonSQL, you can build the TigonSQL Streaming library from the GitHub repo source::

  mvn clean install -DskipTests -P sql-lib,dist

The TigonSQL jar will be installed locally and will be used by maven to create Tigon
applications when they are created on the same machine.

Examples
========

Examples of using Tigon are described in the `Tigon Examples Guide: <examples/index.html>`__

- `Hello World Example <examples/hello-world.html>`__

  .. include:: examples/hello-world.rst
     :start-line: 5
     :end-before: Building the JAR

- `Twitter Analytics Example <examples/twitter-analytics.html>`__

  .. include:: examples/twitter-analytics.rst
     :start-line: 5
     :end-before: Twitter Configuration

- `SQL Join Flow Example <examples/sql-join-flow.html>`__

  .. include:: examples/sql-join-flow.rst
     :start-line: 5
     :end-before: Flow Runtime Arguments


Where to Go Next
================

Now that you've gotten started with Tigon, take a look at:

- `Concepts and Architecture <architecture.html>`__, which covers the basic design behind Tigon.
