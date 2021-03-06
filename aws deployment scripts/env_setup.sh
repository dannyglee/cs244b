# install go
wget -L "https://golang.org/dl/go1.18.1.linux-amd64.tar.gz"
tar -xf "go1.18.1.linux-amd64.tar.gz"
sudo chown -R root:root ./go
sudo mv -v go /usr/local
PATH=$PATH:/usr/local/go/bin
export GOPATH="$HOME/go_projects"
export GOBIN="$GOPATH/bin"
go env -w GO111MODULE=off

# clone raft repo
mkdir -p $HOME/go_projects/src
cd $HOME/go_projects/src
sudo yum install git -y
# need to manually type configure username and password in git
git clone https://github.com/dannyglee/cs244b.git

# start raft process, use different scripts for difference nodes
cd $HOME/go_projects/src/cs244b/aws\ deployment\ scripts/
sh raft_node_1_start.sh

