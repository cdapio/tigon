# SQL Join Flow Example

## Overview

An application that demonstrates the capabilities of the TigonSQL library.
It performs an inner-join of two data streams and logs the result of the SQL command.

## Flow Runtime Arguments

When starting the Application from the command line, runtime arguments will need to be specified.

The only required property is:

```httpPort``` - The port to run the HTTP ingestion endpoints on; example: ```--httpPort=1433```


## Installation

Build the Application jar:

    MAVEN_OPTS="-Xmx512m" mvn package -DskipTests -pl tigon-examples -am -amd -P examples

To start the Flow in the Standalone Runtime of Tigon (substituting for *version* and *httpPort*):

    $ ./run_standalone.sh /path/to/SQLJoinFlow-<version>.jar co.cask.tigon.sqljoinflow.SQLJoinFlow --httpPort=<httpPort>

The Flow exposes 2 ingestion endpoints:

    POST /v1/tigon/ageInput -d { "data" : [ <id>, <age> ] }

    POST /v1/tigon/nameInput -d { "data" : [ <id>, <name> ] }

The output is the inner-join of these two input streams on their ```id```s. A filter of age greater than 40 is applied
on the results.

The output will appear as:

    <id> : <name> : <age>


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
