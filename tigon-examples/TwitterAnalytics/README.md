# Twitter Analytics Example

## Overview

An application that collects Tweets and logs the top 10 hashtags used in the last minute.

## Twitter Configuration

In order to utilize the ``TweetCollector`` flowlet, which pulls a small sample stream via
the Twitter API, an API key and Access token must be configured. Follow the steps at
[Twitter oauth access tokens](https://dev.twitter.com/oauth/overview/application-owner-access-tokens)
to obtain these credentials. These configurations must be provided as runtime arguments to
the Flow prior to starting it in order to use the ``TweetCollector`` flowlet.

## Flow Runtime Arguments

When starting the Application from the command line, runtime arguments may need to be specified.

The required Twitter authorization properties (*oauth-properties*), as described in 
**Twitter Configuration** above, include all of these:

- ```oauth.consumerKey```
- ```oauth.consumerSecret```
- ```oauth.accessToken```
- ```oauth.accessTokenSecret```

## Installation

Build the Application jar:

    mvn clean package

To start the Flow in the Standalone Runtime of Tigon (substituting for *version* and *oauth-properties*):

    $ ./run_standalone.sh /path/to/TwitterAnalytics-<version>.jar co.cask.tigon.analytics.TwitterAnalytics [ oauth-properties ]

To start the Flow in the Distributed Runtime of Tigon (substituting for *version* and *oauth-properties*):

    $ ./run_distributed.sh <ZookeeperQuorum> <HDFSNamespace>
    > START /path/to/TwitterAnalytics-<version>.jar co.cask.tigon.analytics.TwitterAnalytics [ oauth-properties ]

The top ten hashtags used in the previous minute get recorded. In the case of standalone instance of Tigon,
the results will appear immediately in the Tigon command line interface; in the case of distributed instance of Tigon,
the results will be written to the logs of the YARN container of the Flowlet.


## License and Trademarks

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
