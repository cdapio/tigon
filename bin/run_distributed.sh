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

# Attempts to find JAVA in few ways.
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
  CP=""
  for filename in ../lib/*.jar; do
    # Ignore jars that contain "hadoop-" in their name.
    if [[ ! $filename == *hadoop-* ]]; then
      CP="${CP}:${filename}"
    fi
  done
  HBASE_CLASSPATH=`hbase classpath`
  TIGON_CLASSPATH="${CP}:${HBASE_CLASSPATH}"
}

run ()
{
  zkStr=$1
  hdfsNamespace=$2
  shift 2;
  if [[ -z "$zkStr" ]]; then
      echo "ERROR: No ZooKeeperQuorum was given."
      echo "Usage: ./run_distributed.sh <ZooKeeperQuorum> <HDFSNamespace>"
      exit 1
  fi
  if [[ -z "$hdfsNamespace" ]]; then
    echo "ERROR: No HDFSNamespace was given."
    echo "Usage: ./run_distributed.sh <ZooKeeperQuorum> <HDFSNamespace>"
    exit 1
  fi

  "$JAVA" -cp $TIGON_CLASSPATH "co.cask.tigon.DistributedMain" $zkStr $hdfsNamespace
}

set_java
set_classpath

run $@
