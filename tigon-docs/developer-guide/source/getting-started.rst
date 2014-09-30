.. :author: Cask Data, Inc.
   :description: Getting started guide
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Getting Started with Tigon
============================================

Prerequisites
=============

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
========

Pre-compiled sources and related files can be downloaded in a zip file: 
`tigon-0.1.0.zip <http://cask.co/downloads/tigon/tigon-0.1.0.zip>`.


Install 
=======

Once the download has completed, unzip the file to a suitable location.

  
Building Tigon from Source
==========================

.. highlight:: console

You can also get started with Tigon by building directly from the latest source code::

  git clone https://github.com/cask/tigon.git
  cd tigon
  mvn clean package -P sql-lib

After the build completes, you will have a distribution of Tigon under the
`tigon-distribution/target/` directory.  

Take the `tigon-<version>.tar.gz` file and unzip it into a suitable location.


Is It Building?
---------------

- `Bamboo Build <https:////builds.cask.co/browse/TIG>`__
- `GitHub Version <https://github.com/caskco/tigon/releases/latest>`__           

Configuration
=============

Macintosh OS X
--------------

.. highlight:: console

Tigon will not run with the default shared memory settings of the Macintosh, as 
OS X does not provide sufficient shared memory regions by default. 
To run Tigon, you'll need to revise these settings by executing these commands::

  sudo sysctl -w kern.sysv.shmall=102400

  sudo sysctl -w kern.sysv.shmseg=100

  sudo sysctl -w kern.sysv.shmmax=419430400


Problems with the Downloaded Package?
---------------------------------------

If the downloaded package doesn't work, try to build a tarball for your system
as described above.


Where to Go Next
================

Now that you've gotten started with Tigon, take a look at:

- `Concepts and Architecture <architecture.html>`__, which covers the basic design behind Tigon.
