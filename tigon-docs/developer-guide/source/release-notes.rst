.. :author: Cask Data, Inc.
   :description: Release notes for different versions of Tigon
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Release Notes
============================================

Release 0.2.1
=============

Bug Fix
---------

- Fixed a problem in TigonSQL which mapped all schemas to a default interface set,
  resulting in incorrect outputs. Instead, we now have a single interface per input schema
  with no default interface set. Modified Flows that use TigonSQL to adhere to this
  approach.


Release 0.2.0
=============

Features
--------

- Retrieving live logs in distributed mode is available through  the ``showlogs`` CLI command.
- The `Tigon Apps repo <https://github.com/caskdata/tigon-apps>`__ is now available at GitHub. 
  It contains realtime use case applications built using Tigon.
- The ``libcurl`` dependency of TigonSQL has been removed.

Bug Fixes
---------
- A problem with passing Runtime arguments when in Standalone Mode has been fixed.
- A problem with logging when in the Standalone Mode has been fixed.

Known Issues
------------
- The ``libz`` dependency of TigonSQL is only required if running the TigonSQL Stream Engine outside of Tigon.
- See *Known Issues* of `the previous version. <#known-issues-010>`_


Release 0.1.0
=============

Features
--------
- Ability to launch Flows in either Standalone or Distributed Mode. 
- Command Line Interface for Distributed Mode to manage Flows.
- TigonSQL provides integration of In-Memory Streaming Engine through the ``AbstractInputFlowlet``.

.. _known-issues-010:

Known Issues
------------
- In order to use TigonSQL on Mac OS X, shared memory settings need to be modified
  as described in the `Getting Started with Tigon guide. <getting-started.html#macintosh-os-x>`__
- The maximum number of ``AbstractInputFlowlet`` instances is limited to one.
- Metrics are currently not supported.

