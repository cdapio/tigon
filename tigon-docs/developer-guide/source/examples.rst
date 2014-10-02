.. :author: Cask Data, Inc.
   :description: Tigon Examples
   :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Tigon Examples
============================================


TwitterAnalytics
================

Overview
--------
An application that collects Tweets and logs the top ten hashtags used in the previous minute.

Twitter Configuration
---------------------
In order to utilize the ``TweetCollector`` flowlet, which pulls a small sample stream via
the Twitter API, an API key and Access token must be configured. Follow the steps at
`Twitter oauth access tokens
<https://dev.twitter.com/oauth/overview/application-owner-access-tokens>`__ to obtain
these credentials. These configurations must be provided as runtime arguments to the Flow
prior to starting it in order to use the ``TweetCollector`` flowlet.

Flow Runtime Arguments
----------------------

When starting the Application from the command line, runtime arguments may need to be
specified.

The required Twitter authorization properties ("oauth-properties", as described above
under `Twitter Configuration`_) include all of these:

- ``oauth.consumerKey``
- ``oauth.consumerSecret``
- ``oauth.accessToken``
- ``oauth.accessTokenSecret``

.. highlight:: console

Installation
------------

Build the Application jar::

  $ MAVEN_OPTS="-Xmx512m" mvn package -DskipTests -pl tigon-examples -am -amd -P examples


To deploy the Application to a **standalone** instance of Tigon (substituting for *oauth-properties*)::

  $ run_standalone.sh /path/to/TwitterAnalytics-0.1.0.jar co.cask.tigon.analytics.TwitterAnalytics [ oauth-properties ]


To deploy the Application to a **distributed** instance of Tigon (substituting for *oauth-properties*)::

  $ run_distributed.sh <ZookeeperQuorum> <HDFSNamespace>
  
In the command-line shell that starts, enter::
  
  > START /path/to/TwitterAnalytics-0.1.0.jar co.cask.tigon.analytics.TwitterAnalytics [ oauth-properties ]

The top ten hashtags used in the previous minute get recorded. In the case of the
standalone instance of Tigon, the results will appear immediately in the Tigon command
line interface; in the case of the distributed instance of Tigon, the results will be
written to the logs of the YARN container of the Flowlet.


Where to Go Next
================

Now that you're seen an example demonstrating Tigon, take a look at:

- `APIs <apis/apis.html>`__, which includes the Java and Tigon-SQL APIs of Tigon.

