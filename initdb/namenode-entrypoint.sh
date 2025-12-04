#!/bin/bash

# Set proper path for hadoop commands
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

# Check if namenode directory is empty
if [ -z "$(ls -A /hadoop/dfs/name)" ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

echo "Starting NameNode..."
hdfs namenode
