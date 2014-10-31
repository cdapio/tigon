# Hello World Example

## Overview

An application that emits and counts "Hello World" messages.

## Building the JAR

    mvn clean package

To start the Flow in the Standalone Runtime of Tigon (substituting for *version*):

    $ ./run_standalone.sh /path/to/HellWorld-<version>.jar co.cask.tigon.helloworld.HelloWorldFlow 

To start the Flow in the Distributed Runtime of Tigon (substituting for *version*):

    $ ./run_distributed.sh <ZookeeperQuorum> <HDFSNamespace>
    > START /path/to/HellWorld-<version>.jar co.cask.tigon.helloworld.HelloWorldFlow

The HelloWorld Flow logs a "Hello World" message and a total count every second. If run in
the Standalone Runtime, the logs appear on the console. In the Distributed Runtime, the
```showlogs``` CLI command can be used to view these logs.

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
