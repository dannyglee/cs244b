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
sleep 3

node.exe http://localhost:4002 4002 2 > logs/base_case/node2 &
NODE2_PID=$!
sleep 0.1
node.exe http://localhost:4003 4003 3 > logs/base_case/node3 &
NODE3_PID=$!

# brief wait for cluster to stablize
sleep 1

# add user command to system
curl "http://localhost:4001/add?command=testCommand1"
sleep 0.02
curl "http://localhost:4001/add?command=testCommand2"
sleep 0.02
curl "http://localhost:4001/add?command=testCommand3"
sleep 0.02
curl "http://localhost:4001/add?command=testCommand4"
sleep 0.02
curl "http://localhost:4001/add?command=testCommand5"
sleep 0.02

# add new nodes
node.exe http://localhost:4004 4004 4 > logs/base_case/node4 &
NODE4_PID=$!
sleep 0.1
node.exe http://localhost:4005 4005 5 > logs/base_case/node5 &
NODE5_PID=$!
sleep 0.1

# add new user commands. Note that these commands will possily commit before new nodes receive them, but new nodes will eventually receive them.
curl "http://localhost:4001/add?command=newCommand1"
sleep 0.02
curl "http://localhost:4001/add?command=newCommand2"
sleep 0.02
curl "http://localhost:4001/add?command=newCommand3"

# wait for command to commit
sleep 3

# terminate processes and check log
kill -9 $NODE1_PID
kill -9 $NODE2_PID
kill -9 $NODE3_PID
kill -9 $NODE4_PID
kill -9 $NODE5_PID
kill -9 $REGISTRY_PID