#!/bin/bash

#
# Copyright © 2014 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

set_java ()
{
  # Determine the Java command to use to start the JVM.
  if [ -n "$JAVA_HOME" ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        export JAVA="$JAVA_HOME/jre/sh/java"
    else
        export JAVA="$JAVA_HOME/bin/java"
    fi
    if [ ! -x "$JAVA" ] ; then
        echo "ERROR: JAVA_HOME is set to an invalid directory: $JAVA_HOME
Please set the JAVA_HOME variable in your environment to match the
location of your Java installation." >&2
        exit 1
    fi
  else
    export JAVA="java"
    which java >/dev/null 2>&1 || { echo "ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
Please set the JAVA_HOME variable in your environment to match the
location of your Java installation." >&2 ; exit 1; }
  fi
}

set_classpath()
{
  TIGON_CLASSPATH=":../../../lib/*"
  export TIGON_CLASSPATH
}

run ()
{
  port=$1
  jar="../target/SQLJoinFlow-0.1.0.jar"
  classname="co.cask.tigon.sqljoinflow.SQLJoinFlow"
  if [[ -z "$jar" ]] || [[ ! -f "$jar" ]]; then
      echo "ERROR: No jar was given or the jar doesn't exist."
      echo "Usage: ./run_standalone.sh <jar> <classname> [arguments]"
      exit 1
  fi

  echo "Running class $classname"
  "$JAVA" -cp $TIGON_CLASSPATH "co.cask.tigon.StandaloneMain" $jar $classname "--httpPort=$port"
#  sleep 15
}

execute_requests ()
{
  port=$1
  filename="names.data"
  while IFS=, read key value
  do
      echo curl -X POST "http://localhost:$port/v1/tigon/nameInput" -d "{ 'data' : ['$key','$value'] }"
      curl -X POST "http://localhost:$port/v1/tigon/nameInput" -d "{ 'data' : ['$key','$value'] }"
      sleep 1
  done < "$filename"

  sleep 3

  filename="ages.data"
  while IFS=, read key value
  do
      curl -X POST "http://localhost:$port/v1/tigon/ageInput" -d "{ 'data' : ['$key','$value'] }"
      sleep 1
  done < "$filename"
}

set_java
set_classpath

run $@
#execute_requests $@