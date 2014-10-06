.. :author: Cask Data, Inc.
   :description: Release notes for different versions of Tigon
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Release Notes
============================================

Release 0.1.0
=============

Features
--------
- Ability to launch Flows in either Standalone or Distributed Mode. 
- Command Line Interface for Distributed Mode to manage Flows.
- TigonSQL provides integration of In-Memory Streaming Engine through the ``AbstractInputFlowlet``.

Known Issues
------------
- TigonSQL requires ``libcurl`` to be installed on all machines.
- In order to use TigonSQL on Mac OS X, shared memory settings need to be modified
  as described in the `Getting Started with Tigon guide. <getting-started.html#macintosh-os-x>`__
- The maximum number of ``AbstractInputFlowlet`` instances is limited to one.
- Metrics are currently not supported.
- A Flow needs to have at least two Flowlets.


Where to Go Next
================

Now that you're seen the Tigon release notes, take a look at:

- `FAQ <faq.html>`__, which includes answers on Running Tigon, Tigon Support, and Contributing to Tigon.
