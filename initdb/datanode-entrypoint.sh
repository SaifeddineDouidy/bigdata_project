#!/bin/bash

# Set proper path for hadoop commands
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

echo "Starting DataNode..."
hdfs datanode
