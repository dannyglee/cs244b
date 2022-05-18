#!/bin/bash

# create necessary log files
mkdir -p ../node/logs/simple_election && cd "$_"
touch node1
touch node2
touch node3
touch registry_log

# spin up processes
cd ../../../registry
go install
registry.exe > ../node/logs/simple_election/registry_log &
REGISTRY_PID=$!

cd ../node
go install
node.exe http://localhost:4001 4001 1 > logs/simple_election/node1 &
NODE1_PID=$!

# wait to ensure node 1 is leader.
sleep 0.3

node.exe http://localhost:4002 4002 2 > logs/simple_election/node2 &
NODE2_PID=$!
node.exe http://localhost:4003 4003 3 > logs/simple_election/node3 &
NODE3_PID=$!

# wait for system to stablize.
sleep 0.5

# kill leader to trigger new election.
kill -9 $NODE1_PID

# check log to verify new leader election message. Need to check manually because leader election timeouts are random.
sleep 2

kill -9 $NODE2_PID
kill -9 $NODE3_PID
kill -9 $REGISTRY_PID