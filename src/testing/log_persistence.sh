#!/bin/bash

# This test ensures that after a leadership change, any new members joining after sees the full
# log from the previous terms.

# create necessary log files
mkdir -p ../node/logs/log_persistence && cd "$_"


# spin up processes
cd ../../../registry
go install
registry.exe > ../node/logs/log_persistence/registry_log &
REGISTRY_PID=$!

cd ../node
go install
node.exe http://localhost:4001 4001 1 > logs/log_persistence/node1_before &
NODE1_PID=$!

# wait to ensure node 1 is leader.
sleep 0.2

node.exe http://localhost:4002 4002 2 > logs/log_persistence/node2 &
NODE2_PID=$!
node.exe http://localhost:4003 4003 3 > logs/log_persistence/node3 &
NODE3_PID=$!

# add a few log entries and wait for system to stablize.
curl "http://localhost:4001/add?command=testCommand1"
curl "http://localhost:4001/add?command=testCommand2"
curl "http://localhost:4001/add?command=testCommand3"
sleep 0.2

# kill leader to trigger new election, then add it back, as a follower to new leader
kill -9 $NODE1_PID
sleep 0.2
node.exe http://localhost:4001 4001 1 > logs/log_persistence/node1_after &
NODE1_PID=$!
sleep 0.2

# add new member and ensure full log will be updated
node.exe http://localhost:4004 4004 4 > logs/log_persistence/node4 &
NODE4_PID=$!

# check log to verify new leader election message, and ensure logs are updated on new node.
sleep 0.3

kill -9 $NODE1_PID
kill -9 $NODE2_PID
kill -9 $NODE3_PID
kill -9 $NODE4_PID
kill -9 $REGISTRY_PID