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
go run . -node_url=http://54.201.14.44 -registry_url=http://34.222.254.47:5000 -service_port=4000 -node_id=1

# if current server serves registry, run the following:
cd $HOME/go_projects/src/cs244b/src/registry
go install
go run .