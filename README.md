# Tigon

![Tigon Logo](/tigon-docs/developer-guide/source/_images/tigon.png)

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

There are many applications that can take advantage of its features:

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
[Guides and other documentation](http://docs.cask.co/tigon/current/en/index.html).

## Is It Building?

Builds                                                            
------------------------------------------------------------------
[Bamboo Build](https://builds.cask.co/browse/TIG)                 
[GitHub Version](https://github.com/caskdata/tigon/releases/latest) 


## Getting Started

### Prerequisites

Tigon is supported on *NIX systems such as Linux and Macintosh OS X.
It is not supported on Microsoft Windows.

To install and use Tigon and its included examples, there are a few prerequisites:

  1. JDK 6 or JDK 7 (required to run Tigon; note that $JAVA_HOME should be set)
  2. GCC
  3. G++
  4. Apache Maven 3.0+ (required to build the example applications)
  
Note: To run the TigonSQL Stream Engine outside of Tigon, libz, Perl 5.x, and Python 3.x are required.

### Download

Pre-compiled sources and related files can be downloaded in a zip file: 
[tigon-developer-release-0.2.1.zip.](http://repository.cask.co/downloads/co/cask/tigon/tigon-developer-release/0.2.1/tigon-developer-release-0.2.1.zip)

### Install 

Once the download has completed, unzip the file in a suitable location.

### Run Instructions

To run Tigon in standalone mode:

    $ run_standalone.sh <path-to-flow-jar> <flow-class-name> <run-time-args>

To run Tigon in distributed mode:

    $ run_distributed.sh <zookeeper-quorum> <hdfs-namespace>

### Building from Source

You can also build Tigon directly from the latest source code:

    git clone https://github.com/caskdata/tigon.git
    cd tigon
    mvn clean package -DskipTests -Pdist

After the build completes, you will have a distribution of Tigon under the
`tigon-distribution/target/` directory.  

Take the `tigon-sdk-<version>.zip` file and unzip it into a suitable location.


## Getting Started Guide

Visit our web site for a [Getting Started Guide](http://docs.cask.co/docs/tigon/current/en/getting-started.html)
that will guide you through installing Tigon and running an example.  


## Where to Go Next

Now that you've had a look at the Tigon SDK, take a look at:

- Examples, located in the `/tigon-examples` directory of Tigon
- [Online Examples](http://docs.cask.co/tigon/current/en/examples/index.html) 
  (demonstrating basic features of Tigon) are located on-line
- [Reference Applications:](https://github.com/caskdata/tigon-apps)
  - [AdNetworkFlow:](https://github.com/caskdata/tigon-apps/tree/develop/AdNetworkFlow)
    Demonstrates using Tigon to write a realtime bidding (RTB) advertisement framework
  - [ClickStreamFlow:](https://github.com/caskdata/tigon-apps/tree/develop/ClickStreamFlow)
    Demonstrates using TigonSQL to write a webpage click stream data processing application.
- Developer Guides, located in the source distribution in `/tigon-docs/developer-guide/source`
  or [online](http://docs.cask.co/tigon/current/en/developer.html)

## CDAP

Tigon is the realtime stream processing framework used in the Cask Data Application Platform ([CDAP](http://cdap.io)).
CDAP provides a high-level [Dataset](http://docs.cask.co/cdap/current/en/developers-manual/building-blocks/datasets/index.html) abstraction for User Data Stores 
and allows developers to interact with them in their flowlets. In addition to the Dataset abstraction, CDAP integrates Batch Processing with
MapReduce and Spark support. By combining these with security, metrics, and an intuitive GUI, CDAP greatly simplifies Big Data application development. 
If you like the features and architecture of Tigon, we recommend that you take a look
at [CDAP](http://github.com/caskdata/cdap).

## How to Contribute

Interested in helping to improve Tigon? We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

### Bug Reports & Feature Requests

Bugs and suggestions should be made by [filing an Issue](https://issues.cask.co/browse/tigon).

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

Tigon User Group: 
[tigon-user@googlegroups.com](https://groups.google.com/d/forum/tigon-user)

Tigon Development Discussion: 
[tigon-dev@googlegroups.com](https://groups.google.com/d/forum/tigon-dev)


### IRC Channel

Tigon IRC Channel #tigon on irc.freenode.net


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
