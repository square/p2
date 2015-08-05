#!/usr/bin/env bash

# Setup p2 in a fresh CentOS VM. This script assumes that p2 been mounted
# into /usr/local/share/go/src/github.com/square/p2.

set -ex

# Install and setup Go
curl https://storage.googleapis.com/golang/go1.4.2.linux-amd64.tar.gz > go1.4.2-linux-amd64.tar.gz
sudo tar -C /usr/local -xvf go1.4.2-linux-amd64.tar.gz
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

CERPATH=$GOPATH/src/github.com/square/p2/integration/single-slug-deploy/certs
openssl req -x509 -newkey rsa:2048 -keyout $CERTPATH/key.pem -out $CERTPATH/cert.pem -days XXX -subj "$(echo -n "$subj" | tr "\n" "/")"
cp $CERTPATH/cert.pem $CERTPATH/cert2.pem

go version

# Install Godep
sudo yum install -y mercurial
go get github.com/tools/godep

# Build p2.
cd $GOPATH/src/github.com/square/p2
godep go install ./...
cp $GOPATH/bin/p2-exec /usr/local/bin

# Install ruby + rake
sudo yum install -y ruby rubygem-rake

# Install P2 test dependencies
sudo yum -y --nogpgcheck localinstall $GOPATH/src/github.com/square/p2/integration/test-deps/*rpm
sudo mkdir -p /etc/servicebuilder.d
sudo mkdir -p /var/service-stage
