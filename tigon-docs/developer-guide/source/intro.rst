.. :author: Cask Data, Inc.
   :description: Index document
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Introduction
============================================

Tigon is a high-speed stream database. Tigon's original purpose was network monitoring, though
there are many other applications that can take advantage of its features:

- Ability to handle extremely large data flows;
- Data is touched only once, useless data is not gathered, and results are quickly produced;
- Runs collections of queries using pipelined query plans and a SQL-like language;
- Supports a significant amount of parallelization; and 
- Able to transparently handle complex record routing in large parallelized implementations.

Getting Started
---------------

Prerequisites
.............

Tigon is supported on *NIX systems such as Linux and Macintosh OS X.
It is not supported on Microsoft Windows.

To install and use Tigon and its included examples, there are a few prerequisites:

  1. JDK 6 or JDK 7 (required to run Tigon; note that $JAVA_HOME should be set)
  2. GCC
  3. G++
  4. libcurl (if it is not already included in your operating system)
  5. Apache Maven 3.0+ (required to build the example applications)
  
### Build

You can get started with Tigon by building directly from the latest source code::

```
  git clone https://github.com/cask/tigon.git
  cd tigon
  mvn clean package
```

After the build completes, you will have a distribution of Tigon under the
`tigon-distribution/target/` directory.  

Take the `tigon-<version>.tar.gz` file and unzip it into a suitable location.

For more build options, please refer to the [build instructions](BUILD.md).


## Quick Start

Visit our web site for a [Quick Start](http://docs.cask.co/docs/tigon/current/quickstart.html)
that will guide you through installing Tigon, running an example that....  


## Where to Go Next

Now that you've had a look at Tigon SDK, take a look at:

- Examples, located in the `/tigon-examples` directory of Tigon;
- [Selected Examples](http://docs.cask.co/tigon/current/examples.html) 
  (demonstrating basic features of Tigon) are located on-line; and
- Developer Guides, located in the source distribution in `/docs/developer-guide/source`
  or [online](http://docs.cask.co/tigon/current/index.html).


Where to Go Next
================
Now that you've had an introduction to Tigon, take a look at:

- `Concepts <concepts.html>`__, which covers the basic concepts behind Tigon.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :trim: