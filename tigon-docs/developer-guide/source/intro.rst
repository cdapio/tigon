.. :author: Cask Data, Inc.
   :description: Introduction
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
low-latency stream processing with a SQL-like query language.

Together, they have combined to create **Tigon**.

There are many applications that can take advantage of its features:

- Ability to handle **extremely large data flows**;
- **Exactly-once event processing** using an app-level Java API with consistency, reliability, and persistence;
- **Streaming database using a SQL-like language** to filter, group and join data streams in-memory;
- Runs collections of queries using **pipelined query plans**;
- Able to transparently handle **complex record routing** in large parallelized implementations;
- **Runs and scales** as a native Apache Hadoop |(TM)| YARN Application;
- Reads, writes, and **tightly integrates with HDFS and HBase**;
- Supports a significant amount of **parallelization**;
- **Fault-tolerance and horizontal scalability** without burdening the developer;
- **Enterprise security features** with debugging, logging, and monitoring tools;
- **Simpler programming model**, tools and UI; and 
- **Open-source software** and development process.



Where to Go Next
================

Now that you've had an introduction to Tigon, take a look at:

- `Getting Started with Tigon <getting-started.html>`__.

.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim: