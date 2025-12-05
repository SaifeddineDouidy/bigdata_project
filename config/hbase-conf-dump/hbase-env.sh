#!/bin/bash
# HBase Environment Configuration
# This file is sourced by HBase startup scripts

# Do not manage ZooKeeper - use external ZooKeeper
export HBASE_MANAGES_ZK=false

# Java options for HBase
# The harisekhon/hbase image ships Java under /usr; keep this in sync with docker-compose.
export JAVA_HOME=${JAVA_HOME:-/usr}

# HBase configuration directory (match docker-compose and image defaults)
export HBASE_CONF_DIR=${HBASE_CONF_DIR:-/hbase/conf}

# Hadoop configuration directory
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/hbase/conf}

# HBase log directory
export HBASE_LOG_DIR=${HBASE_LOG_DIR:-/opt/hbase/logs}

# HBase PID directory
export HBASE_PID_DIR=${HBASE_PID_DIR:-/opt/hbase/pids}

