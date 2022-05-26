#!/bin/sh
PATH=$PATH:/usr/local/go/bin
export GOPATH="$HOME/go_projects"
export GOBIN="$GOPATH/bin"
go env -w GO111MODULE=off

cd $HOME/go_projects/src/cs244b
git pull

cd src/node
go install
# update the flag values when setting it up on difference machines.
go run . -node_url=http://172.31.7.100 -registry_url=http://172.31.9.26:5000 -service_port=4000 -node_id=2