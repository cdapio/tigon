.. :author: Cask Data, Inc.
   :description: Introduction
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Introduction to Tigon
============================================

**Tigon** is an open-source, real-time, low-latency, high-throughput stream processing framework.

Tigon is a collaborative effort between Cask Data, Inc. and AT&T that combines 
technologies from these companies to create a disruptive new framework to handle a diverse
set of real-time streaming requirements.

Cask Data has built technology that provides scalable, reliable, and persistent high-throughput
event processing with high-level Java APIs using Apache |(TM)| Hadoop |(R)| and HBase.

AT&T has built a streaming engine that provides massively scalable, flexible, and in-memory
low-latency stream processing with a SQL-like query language.

Together, they have combined to create **Tigon**.

There are many applications that can take advantage of its features:

- Ability to handle **extremely large data flows**;
- **Exactly-once event processing** using an app-level Java API with consistency, reliability, and persistence;
- **Streaming database using a SQL-like language** to filter, group and join data streams in-memory;
- Runs collections of queries using **pipelined query plans**;
- Able to transparently handle **complex record routing** in large parallelized implementations;
- **Runs and scales** as a native Apache Hadoop YARN Application;
- Reads, writes, and **tightly integrates with HDFS and HBase**;
- Supports a significant amount of **parallelization**;
- **Fault-tolerance and horizontal scalability** without burdening the developer;
- **Enterprise features** with logging, and monitoring tools;
- **Simpler programming model**, tools and UI; and 
- **Open-source software** and development process.


Where to Go Next
================

.. |getting-started| replace:: **Getting Started with Tigon:**
.. _getting-started: getting-started.html

- |getting-started|_ covers **prerequisites, download instructions, installation, building and configuration.** 


.. |concepts| replace:: **Concepts and Architecture:**
.. _concepts: concepts.html

- |concepts|_ explains the **overall architecture and technology stack** behind Tigon.


.. |developer| replace:: **Developer Guide:**
.. _developer: developer.html

- |developer|_ the components of Tigon: **Flows; Flowlets; Queues;** and the **Flow Transaction System** along with 
  suggested **best practices for application development.**


.. |examples| replace:: **Examples and Applications:**
.. _examples: examples/index.html

- |examples|_ **Tigon examples** are included in the source distribution in the ``tigon-examples`` directory;
  **example applications** are online in the `tigon-apps repository <https://github.com/caskdata/tigon-apps>`__.


.. |apis| replace:: **APIs:**
.. _apis: apis/index.html

- |apis|_  includes the **Java** and **TigonSQL APIs** of Tigon, including documentation
  for developers writing extensions to TigonSQL.


.. |tools| replace:: **Tools:**
.. _tools: tools.html

- |tools|_ describes the **Tigon Command-Line Interface,** which provides methods to interact with a running 
  Tigon instance from within a shell, similar to an HBase shell or bash.


.. |admin| replace:: **Administration:**
.. _admin: admin.html

- |admin|_ explains the commands for **starting and stopping Tigon,** in both Standalone and Distributed modes.


.. |advanced| replace:: **Advanced Features:**
.. _advanced: advanced.html

- |advanced|_ covers **TigonSQL,** the high-performance data stream management system inside Tigon. You can
  use Tigon and develop applications without reference to this section.


.. |remaining| replace:: **Remaining Sections:**

- |remaining| the final documentation sections cover the :doc:`Licenses and Copyrights,
  <licenses>` :doc:`Releases Notes, <release-notes>` and a :doc:`FAQ. <faq>`



.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:

.. |(R)| unicode:: U+00AE .. registered trademark sign
   :ltrim:
