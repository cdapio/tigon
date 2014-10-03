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
#. libcurl (if it is not already included in your operating system; see note following)
#. libz (if it is not already included in your operating system; see note following)
#. Apache Maven 3.0+ (required to build the example applications)
#. In standalone mode, both Perl 5.x and Python 3.x are required

**Note:** You need the ``libcurl.so`` and ``libz.so`` at runtime, which are provided by
the ``libcurl`` and ``libz`` packages. At compile time, you need the ``curl/curl.h`` and
``zlib.h`` headers, provided by ``libcurl-devel`` and ``zlib-devel`` on RHEL systems and
provided by ``libcurl-dev`` and ``libz-dev`` on Debian-based systems. Users who provide
curl and libz via compilation (download ``curl`` and ``libz`` tarballs, ``./configure &&
make && make install``) will have the headers provided during the make install.

**Note:** To run the TigonSQL Stream Engine outside of Tigon, both Perl 5.x and Python 3.x
are required.


Download
========

Pre-compiled sources and related files can be downloaded in a zip file: 
`tigon-0.1.0.zip. 
<http://repository.cask.co/downloads/co/cask/tigon/tigon-developer-release/0.1.0/tigon-developer-release-0.1.0.tgz>`__


Install 
=======

Once the download has completed, unzip the file to a suitable location.

  
Building Tigon from Source
==========================

.. highlight:: console

You can also build Tigon directly from the latest source code::

  git clone https://github.com/caskdata/tigon.git
  cd tigon
  mvn clean package -P sql-lib -DskipTests

After the build completes, you will have a distribution of Tigon under the
`tigon-distribution/target/` directory.  

Take the `tigon-<version>.tar.gz` file and unzip it into a suitable location.


Is It Building?
---------------

- `Bamboo Build <https://builds.cask.co/browse/TIG>`__
- `GitHub Version <https://github.com/caskdata/tigon/releases/latest>`__           


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


Examples
========

An example of using Tigon with TwitterAnalytics is described in the `Tigon Examples Guide.
<examples.html>`__

It's an application that collects Tweets and logs the top ten hashtags used in the
previous minute.


Where to Go Next
================

Now that you've gotten started with Tigon, take a look at:

- `Concepts and Architecture <architecture.html>`__, which covers the basic design behind Tigon.
