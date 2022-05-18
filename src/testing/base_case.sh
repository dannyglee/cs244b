#!/bin/bash

# This file includes a comprehensive correctness test.

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
sleep 0.3

node.exe http://localhost:4002 4002 2 > logs/base_case/node2 &
NODE2_PID=$!
node.exe http://localhost:4003 4003 3 > logs/base_case/node3 &
NODE3_PID=$!

# add user command to system
curl "http://localhost:4001/add?command=testCommand1"
curl "http://localhost:4001/add?command=testCommand2"
curl "http://localhost:4001/add?command=testCommand3"

# add new nodes
node.exe http://localhost:4004 4004 4 > logs/base_case/node4 &
NODE4_PID=$!
node.exe http://localhost:4005 4005 5 > logs/base_case/node5 &
NODE5_PID=$!

# add new user commands. Note that these commands will possily commit before new nodes receive them, but new nodes will eventually receive them.
curl "http://localhost:4001/add?command=newCommand1"
curl "http://localhost:4001/add?command=newCommand2"
curl "http://localhost:4001/add?command=newCommand3"

# kill off 2 nodes and make sure client requests can still commit. One of the killed off nodes is leader.
sleep 2
kill -9 $NODE2_PID
kill -9 $NODE3_PID

curl "http://localhost:4001/add?command=afterkill1"
curl "http://localhost:4001/add?command=afterkill2"
curl "http://localhost:4001/add?command=afterkill3"

# kill off 1 more node, and ensure requests won't commit.
sleep 2
kill -9 $NODE4_PID

curl "http://localhost:4001/add?command=nocommit1"
curl "http://localhost:4001/add?command=nocommit2"

# wait for system to stablize.
sleep 2

# terminate processes and check log
kill -9 $NODE1_PID
kill -9 $NODE5_PID
kill -9 $REGISTRY_PID