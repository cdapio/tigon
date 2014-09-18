# Tigon

![Tigon Logo](/docs/guides/source/_images/tigon.png)

**Introduction**

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
- Runs and scales as a native Apache Hadoop YARN Application;
- Reads, writes, and tightly integrates with HDFS and HBase;
- Supports a significant amount of parallelization;
- Fault-tolerance and horizontal scalability without burdening the developer;
- Enterprise security features with debugging, logging, and monitoring tools; and
- Simpler programming model, tools and UI; and
- Open-source software and development process.

For more information, see our collection of 
[Guides and other documentation](http://docs.cask.co/tigon/current/index.html).

## Is It Building?

Build                                                                     | Status / Version
--------------------------------------------------------------------------|-----------------
[Travis Continuous Integration Build](https://travis-ci.org/caskco/tigon) | ![travis](https://travis-ci.org/caskco/tigon.svg?branch=develop)
[GitHub Version](https://github.com/caskco/tigon/releases/latest)         | ![github](http://img.shields.io/github/release/caskco/tigon.svg)


## Getting Started

### Prerequisites

Tigon is supported on *NIX systems such as Linux and Macintosh OS X.
It is not supported on Microsoft Windows.

To install and use Tigon and its included examples, there are a few prerequisites:

  1. JDK 6 or JDK 7 (required to run Tigon; note that $JAVA_HOME should be set)
  2. GCC
  3. G++
  4. libcurl (if it is not already included in your operating system)
  5. libz
  6. Apache Maven 3.0+ (required to build the example applications)
  7. In standalone mode, both Perl 5.x and Python 3.x are required


### Download

Pre-compiled sources and related files can be downloaded in a zip file: 
[tigon-0.1.0.zip] (http://cask.co/downloads/tigon/tigon-0.1.0.zip).


### Install 

Once the download has completed, unzip the file in a suitable location.


### Building from Source

You can also get started with Tigon by building directly from the latest source code::

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


## How to Contribute

Interested in helping to improve Tigon? We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

### Bug Reports & Feature Requests

Bugs and tasks are tracked in a public JIRA issue tracker. Details on access will be forthcoming.

### Pull Requests

We have a simple pull-based development model with a consensus-building phase, similar to Apache's
voting process. If you’d like to help make Tigon better by adding new features, enhancing existing
features, or fixing bugs, here's how to do it:

1. If you are planning a large change or contribution, discuss your plans on the `cask-tigon-dev`
   mailing list first.  This will help us understand your needs and best guide your solution in a
   way that fits the project.
2. Fork Tigon into your own GitHub repository.
3. Create a topic branch with an appropriate name.
4. Work on the code to your heart's content.
5. Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in
   all of the description fields).
6. After we review and accept your request, we’ll commit your code to the cask/tigon
   repository.

Thanks for helping to improve Tigon!

### Mailing List

Tigon User Group and Development Discussions: 
[tigon-dev@googlegroups.com](https://groups.google.com/d/forum/tigon-dev)

### IRC Channel

Tigon IRC Channel #cask-tigon on irc.freenode.net


## License and Trademarks

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.
