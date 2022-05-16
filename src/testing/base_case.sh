#!/bin/bash

# create necessary log files
mkdir -p ../node/logs/base_case && cd "$_"
touch node1
touch node2
touch node3
touch registry_log

# spin up processes
cd ../../../registry
go install
registry.exe > ../node/logs/base_case/registry_log &
REGISTRY_PID=$!

cd ../node
go install
node.exe http://localhost:4001 4001 1 > logs/base_case/node1 &
NODE1_PID=$!

# wait to ensure node 1 is leader
sleep 3 & WAIT_PID=$!
wait $WAIT_PID

node.exe http://localhost:4002 4002 2 > logs/base_case/node2 &
NODE2_PID=$!

node.exe http://localhost:4003 4003 3 > logs/base_case/node3 &
NODE3_PID=$!

# brief wait for cluster to stablize
sleep 5 & WAIT_PID=$!
wait $WAIT_PID

# add user command to system
curl "http://localhost:4001/add?command=testCommand$i"

# wait for command to commit
sleep 3 & WAIT_PID=$!
wait $WAIT_PID

# terminate processes and check log
kill -9 $NODE1_PID
kill -9 $NODE2_PID
kill -9 $NODE3_PID
kill -9 $REGISTRY_PID