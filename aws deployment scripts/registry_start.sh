#!/bin/sh
PATH=$PATH:/usr/local/go/bin
export GOPATH="$HOME/go_projects"
export GOBIN="$GOPATH/bin"
go env -w GO111MODULE=off

cd $HOME/go_projects/src/cs244b
git pull

# if current server serves registry, run the following:
cd $HOME/go_projects/src/cs244b/src/registry
go install
go run . &