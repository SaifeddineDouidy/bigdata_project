#!/bin/bash
echo "creating 'test_hbase', 'cf'" | hbase shell
echo "put 'test_hbase', 'row1', 'cf:a', 'value1'" | hbase shell
echo "scan 'test_hbase'" | hbase shell
