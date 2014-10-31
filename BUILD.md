# Tigon

**Standalone and Distributed Tigon**

## Building Tigon Maven

### Clean all modules
    mvn clean

### Build all modules
    MAVEN_OPTS="-Xmx3056m  -XX:MaxPermSize=128m" mvn package

### Run checkstyle, skipping test
    mvn package -DskipTests

### Build a particular module
    mvn package -pl [module] -am

### Run selected test
    mvn -Dtest=TestClass,TestMore*Class,TestClassMethod#methodName -DfailIfNoTests=false test

See [Surefire doc](http://maven.apache.org/surefire/maven-surefire-plugin/examples/single-test.html) for more details

### Build all examples
    mvn package -DskipTests -pl tigon-examples -am -amd -P examples

### Build Tigon Distribution
    mvn clean package -DskipTests -P examples -pl tigon-examples -am -amd && mvn package -DskipTests -Pdist

### Rebuild TigonSQL Library
    mvn clean package install -DskipTests -Psql-lib

### Build the complete set of Javadocs, for all modules
    mvn clean site -DskipTests

### Show dependency tree
    mvn package dependency:tree -DskipTests

### Show dependency tree for a particular module
    mvn package dependency:tree -DskipTests -pl [module] -am

### Show test output to stdout
    mvn -Dsurefire.redirectTestOutputToFile=false ...

### Offline mode
    mvn -o ....

## License and Trademarks

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.
