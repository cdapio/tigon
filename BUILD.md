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

### Build Tigon Distribution ZIP
    mvn clean package -DskipTests -Pdist

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