.. :author: Cask Data, Inc.
   :description: FAQ of Tigon
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon FAQ
============================================

General
=======

**What is Tigon?**

**Tigon** is an open-source, real-time stream processing framework built on top of Hadoop |(TM)| and HBase.

**Who is Tigon intended for?**

Developers who are interested in creating powerful, yet simple-to-develop stream processing
applications that can handle large volumes of data.

**What are some of the applications that can be built on Tigon?**

- Processing stream sources such as Twitter, Webserver Logs
- Rapid Joining, Filtering, and Aggregating of Streams

**How does Tigon work?**

Tigon is built on top of Hadoop/HBase. It uses the 
`Tephra Transaction Engine <https://github.com/caskdata/tephra>`__ and Twill
for spinning up YARN applications. For in-memory stream processing, it uses the TigonSQL
in-memory stream processing engine developed by AT&T.

**What is the difference between Tigon and Cask's other projects, such as CDAP?**

Tigon is focused on solving the issues faced in real-time stream processing. 

The `Cask Data Application Platform (CDAP) <http://cdap.io>`__ is a generalized 'Big
Data' application platform with additional features such as dataset abstractions, batch job
integration, and security.

**What's the vision for Tigon?**

Enable every Java developer to create powerful, real-time stream processing applications.

.. **How fast will Tigon import data?**

.. **How big a cluster has Tigon been run on?**


Running Tigon
=============

**How do I get started with Tigon?**

Download the tar.gz, untar it and checkout the examples! 

See our `Getting Started With Tigon <getting-started.html>`__ guide.

**What are the prerequisites for running Tigon?**

See our `Getting Started With Tigon <getting-started.html#prerequisites>`__ guide
for a list of prerequisites.

**What platforms and Java version does Tigon run on?**

Tigon runs on \*NIX systems such as Linux and Macintosh OS X.
A Java Development Kit such as JDK 6 or JDK 7 is required to run Tigon.

.. **Are there Tigon RPM or Debian packages available for download?**

**Does Tigon run on Windows?**

Currently, Tigon does not run on Windows.

**What hardware do I need for Tigon?**

Tigon runs on the same hardware that would support Hadoop/HBase.

**What programming languages are supported by Tigon?**

Applications that use Tigon currently need to be written in Java. 
If you are using TigonSQL, commands are written in an SQL dialect.

If you are running TigonSQL in standalone mode or are running unit tests,
certain elements are written in Perl and Python.

Tigon Support
=========================

**Where can I find more information about Tigon?**

Our resources include this website, our parent website (`cask.co <http://cask.co>`__),
a mailing list, an IRC channel and a `GitHub repository. <https://github.com/caskdata/tigon>`__

**What mailing lists are available for additional help?**

Tigon User Group and Development Discussions: 

- `tigon-dev@googlegroups.com <https://groups.google.com/d/forum/tigon-dev>`__
- `tigon-user@googlegroups.com <https://groups.google.com/d/forum/tigon-user>`__

**Is there an IRC Channel?**

Tigon IRC Channel #tigon on irc.freenode.net

**Where are Bug Reports and Feature Requests kept?**

Bugs and tasks are tracked in a public JIRA issue tracker. Details on access will be forthcoming.

**Is commercial support available for Tigon?**

Contact Cask Data for information on `commercial Tigon support. <http:cask.co/support>`__


Contributing to Tigon
=========================

**How can I help make Tigon better?**

We welcome all contributions, whether in filing detailed
bug reports, submitting pull requests for code changes and improvements, or by asking questions and
assisting others on the mailing list.

**How can I contribute?**

Are you interested in making Tigon better? We have a simple pull-based development model
with a consensus-building phase, similar to Apache's voting process. If you’d like to help
make Tigon better by adding new features, enhancing existing features, or fixing bugs,
here's how to do it:

1. If you are planning a large change or contribution, discuss your plans on the `tigon-dev`
   mailing list first.  This will help us understand your needs and best guide your solution in a
   way that fits the project.
#. Fork `Tigon <https://github.com/caskdata/tigon>`__ into your own GitHub repository.
#. Create a topic branch with an appropriate name.
#. Work on the code to your heart's content.
#. Once you’re satisfied, create a pull request from your GitHub repo (it’s helpful if you fill in
   all of the description fields).
#. Address all the review comments.
#. After we review and accept your request, we’ll commit your code to the 
   `caskdata/tigon <https://github.com/caskdata/tigon>`__ repository.
   
.. |(TM)| unicode:: U+2122 .. trademark sign
   :ltrim:
