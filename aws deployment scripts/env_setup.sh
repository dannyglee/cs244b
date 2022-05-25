# install go
wget -L "https://golang.org/dl/go1.18.1.linux-amd64.tar.gz"
tar -xf "go1.18.1.linux-amd64.tar.gz"
sudo chown -R root:root ./go
sudo mv -v go /usr/local
export  PATH=$PATH:/usr/local/go/bin
export GOPATH="$HOME/go_projects"
export GOBIN="$GOPATH/bin"
go env -w GO111MODULE=off

# clone raft repo
mkdir -p %HOME/go_projects/src
cd %HOME/go_projects/src
sudo yum install git -y
git clone https://github.com/dannyglee/cs244b.git

# install and run the program
cd cs244b/src/node
go install
go run . 