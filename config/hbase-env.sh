#!/bin/bash
# HBase Environment Configuration
# This file is sourced by HBase startup scripts

# Do not manage ZooKeeper - use external ZooKeeper
export HBASE_MANAGES_ZK=false

# Java options for HBase
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}

# HBase configuration directory
export HBASE_CONF_DIR=${HBASE_CONF_DIR:-/opt/hbase/conf}

# Hadoop configuration directory
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/opt/hbase/conf}

# HBase log directory
export HBASE_LOG_DIR=${HBASE_LOG_DIR:-/opt/hbase/logs}

# HBase PID directory
export HBASE_PID_DIR=${HBASE_PID_DIR:-/opt/hbase/pids}

