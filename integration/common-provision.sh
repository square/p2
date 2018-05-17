#!/usr/bin/env bash

# Setup p2 in a fresh CentOS VM. This script assumes that p2 been mounted
# into /usr/local/share/go/src/github.com/square/p2.

set -ex

# Install and setup Go
export VERSION=1.9.2
curl -LO https://storage.googleapis.com/golang/go$VERSION.linux-amd64.tar.gz
sudo tar -C /usr/local -xvf go$VERSION.linux-amd64.tar.gz
sudo mkdir -p /usr/local/share/go
sudo chown vagrant:vagrant /usr/local/share/go
sudo sh -c 'echo "GOPATH=/usr/local/share/go" >> /etc/environment'
export GOPATH=/usr/local/share/go
export PATH="/usr/local/go/bin:/usr/local/share/go/bin:$PATH"
sudo sh -c "echo 'export PATH=$PATH' > /etc/profile.d/gopath.sh"
sudo sh -c "echo 'export GOPATH=/usr/local/share/go' >> /etc/profile.d/gopath.sh"

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
cd ${GOPATH}/src/github.com/square/p2
go install ./...
cp ${GOPATH}/bin/p2-exec /usr/local/bin

# Install P2 test dependencies
sudo yum -y --nogpgcheck localinstall $GOPATH/src/github.com/square/p2/integration/test-deps/*rpm
sudo yum -y install unzip
sudo mkdir -p /etc/servicebuilder.d
sudo mkdir -p /var/service-stage

# symlink cgroup from modern /sys/fs/cgroup to legacy /cgroup
sudo ln -sf /sys/fs/cgroup /cgroup

# build the opencontainer hello container that the integration test uses
pushd /usr/local/share/go/src/github.com/square/p2/integration/hello
go build
popd

# make the artifact in /tmp because the docker image has hardlinks which can't
# go on the shared directory (/usr/local/share/go/src/github.com/square/p2)
pushd /tmp
mkdir -p opencontainer-hello_def456/rootfs
cp /usr/local/share/go/src/github.com/square/p2/integration/hello/config.json opencontainer-hello_def456/

# install docker using instructions from
# https://docs.docker.com/install/linux/docker-ce/centos/#set-up-the-repository
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
sudo yum-config-manager \
	--add-repo \
	https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install -y docker-ce

sudo systemctl start docker
sudo docker pull centos
container_id=$(sudo docker create centos:latest)
sudo docker export $container_id > centos.tar

# this creates a rootfs directory in opencontainer-hello_def456/
tar xf centos.tar -C opencontainer-hello_def456/rootfs
mkdir -p opencontainer-hello_def456/rootfs/usr/local/bin

mv /usr/local/share/go/src/github.com/square/p2/integration/hello/hello opencontainer-hello_def456/rootfs/usr/local/bin
tar czf opencontainer-hello_def456.tar.gz -C opencontainer-hello_def456 .
gpg --no-tty --yes --no-default-keyring --keyring /usr/local/share/go/src/github.com/square/p2/integration/single-node-slug-deploy/pubring.gpg \
  --secret-keyring /usr/local/share/go/src/github.com/square/p2/integration/single-node-slug-deploy/secring.gpg -u p2universe --out \
  opencontainer-hello_def456.tar.gz.sig --detach-sign opencontainer-hello_def456.tar.gz
popd

curl -L https://github.com/opencontainers/runc/releases/download/v1.0.0-rc4/runc.amd64 -o /usr/local/bin/runc

chmod +x /usr/local/bin/runc
