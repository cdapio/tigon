# SQLJoinFlow

## Overview
An application that demonstrates the capabilities of tigon-sql library.
It does an inner-join of two data streams and logs the result of the SQL command.

## Flow Runtime Arguments
When starting the Application from the command line, runtime arguments will need to be specified.

The only required property is:

"httpPort" - The port to run the HTTP ingestion endpoints on.

To deploy the Application to a standalone instance of Tigon:
```
$ ./run_standalone.sh /path/to/SQLJoinFlow-0.1.0.jar cco.cask.tigon.sqljoinflow.SQLJoinFlow [ host-property ]
```

The Flow exposes 2 ingestion endpoints:
```
POST /v1/tigon/ageInput -d { "data" : [ <id>, <age> ] }
```

```
POST /v1/tigon/nameInput -d { "data" : [ <id>, <name> ] }
```

The output is the inner-join of these two input streams on their ``id``s. A filter of age greater than 40 is applied
on the results.

The output will look like:

```
<id> : <name> : <age>
```

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
