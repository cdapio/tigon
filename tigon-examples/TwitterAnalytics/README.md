SentimentAnalysis
=================
Sentiment Analysis application.

Overview
========
An application that analyzes the sentiments of Twitter Tweets and categorizes them as either positive, negative or neutral.

Twitter Configuration
=====================
In order to utilize the TweetCollector flowlet, which pulls a small sample stream via the Twitter API, the API key and Access token must be configured.
Follow the steps at the following page to obtain these credentials: [Twitter oauth access tokens](https://dev.twitter.com/oauth/overview/application-owner-access-tokens).
These configurations must be provided as runtime arguments to the flow prior to starting it, in order to use the TweetCollector flowlet.

Flow Runtime Arguments
======================
When starting the ```analysis``` flow from the command line, runtime arguments can be specified.
"oauth.consumerKey" - See ```Twitter Configuration``` above.
"oauth.consumerSecret" - See ```Twitter Configuration``` above.
"oauth.Token" - See ```Twitter Configuration``` above.
"oauth.TokenSecret" - See ```Twitter Configuration``` above.

Installation
============

Build the Application jar:
```
mvn clean package
```

To deploy the Application to a standalone instance of Tigon:
```
bin/bash run_standalone.sh SentimentAnalysis-0.1.0.jar co.cask.tigon.sentiment.SentimentAnalysis [ oauth-properties ]
```

To deploy the Application to a distributed instance of Tigon:
```
bin/bash run_distributed.sh <ZookeeperQuorum> <HDFSNamespace>
> START SentimentAnalysis-0.1.0.jar co.cask.tigon.sentiment.SentimentAnalysis [ oauth-properties ]
```

The sentiment classifications get printed to a log file.

## License and Trademarks

Copyright 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
