#!/usr/bin/env bash

# Setup p2 in a fresh CentOS VM. This script assumes that p2 been mounted
# into /usr/local/share/go/src/github.com/square/p2.

set -ex

# Install and setup Go
VERSION=1.9.2
curl -LO https://storage.googleapis.com/golang/go$VERSION.linux-amd64.tar.gz
sudo tar -C /usr/local -xvf go$VERSION.linux-amd64.tar.gz
sudo mkdir -p /usr/local/share/go
sudo chown vagrant:vagrant /usr/local/share/go
sudo sh -c 'echo "GOPATH=/usr/local/share/go" >> /etc/environment'
export GOPATH=/usr/local/share/go
export PATH="/usr/local/go/bin:/usr/local/share/go/bin:$PATH"
sudo sh -c "echo 'export PATH=$PATH' > /etc/profile.d/gopath.sh"

# make ssl certs
subj="
C=US
ST=CA
O=SQ
localityName=SF
commonName=$HOSTNAME
organizationalUnitName=Vel
emailAddress=doesntmatter@something.edu
"

CERTPATH=/var/tmp/certs
mkdir -p $CERTPATH
openssl req -x509 -newkey rsa:2048 -keyout $CERTPATH/key.pem -out $CERTPATH/cert.pem -nodes -days 300 -subj "$(echo -n "$subj" | tr "\n" "/")"

go version

# check.go assumes rake to be present (it invokes `rake install`)
# gcc is required to `go install` certain packages, probably those that `import "C"`
# (p2/bin/p2-exec, opencontainers/runc/libcontainer/system, mattn/go-sqlite3)
# git is required to `go get`
sudo yum install -y rubygem-rake gcc git

# Build p2.
cd $GOPATH/src/github.com/square/p2
go install ./...
cp $GOPATH/bin/p2-exec /usr/local/bin

# Install P2 test dependencies
sudo yum -y --nogpgcheck localinstall $GOPATH/src/github.com/square/p2/integration/test-deps/*rpm
sudo yum -y install unzip
sudo mkdir -p /etc/servicebuilder.d
sudo mkdir -p /var/service-stage
