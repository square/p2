#!/usr/bin/env bash

set -ex

sudo groupadd nobody
sudo useradd hello
sudo useradd p2-rctl-server
sudo useradd label-store-server

sudo /sbin/stop runsvdir
sudo sed -i -e 's;/usr/sbin/runsvdir-start;/usr/bin/runsvdir /var/service;g' /etc/init/runsvdir.conf
sudo /sbin/start runsvdir
sudo mkdir -p /etc/servicebuilder.d /var/service-stage /var/service

sudo cp $GOPATH/bin/p2-exec /usr/local/bin
PATH=$PATH:$GOPATH/bin

# At some point travis changed so that directories have 750 permissions, but we
# need +x so we can traverse to the files underneath
sudo chmod 755 /home
sudo chmod 755 /home/travis
sudo chmod 755 $GOPATH
sudo chmod 755 $GOPATH/bin

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

# symlink cgroup from modern /sys/fs/cgroup to legacy /cgroup
sudo ln -sf /sys/fs/cgroup /cgroup

# build the opencontainer hello container that the integration test uses
pushd $GOPATH/src/github.com/square/p2/integration/hello
go build
popd

# make the artifact in /tmp because the docker image has hardlinks which can't
# go on the shared directory ($GOPATH/src/github.com/square/p2)
pushd /tmp
mkdir -p opencontainer-hello_def456/rootfs
cp $GOPATH/src/github.com/square/p2/integration/hello/config.json opencontainer-hello_def456/

# get a suitable root filesystem for centos from docker
sudo docker pull centos
container_id=$(sudo docker create centos:latest)
sudo docker export $container_id > centos.tar

# this creates a rootfs directory in opencontainer-hello_def456/
sudo tar xf centos.tar -C opencontainer-hello_def456/rootfs
mkdir -p opencontainer-hello_def456/rootfs/usr/local/bin

sudo mv $GOPATH/src/github.com/square/p2/integration/hello/hello opencontainer-hello_def456/rootfs/usr/local/bin
sudo tar czf opencontainer-hello_def456.tar.gz -C opencontainer-hello_def456 .
gpg --no-tty --yes --no-default-keyring --keyring $GOPATH/src/github.com/square/p2/integration/single-node-slug-deploy/pubring.gpg \
  --secret-keyring $GOPATH/src/github.com/square/p2/integration/single-node-slug-deploy/secring.gpg -u p2universe --out \
  opencontainer-hello_def456.tar.gz.sig --detach-sign opencontainer-hello_def456.tar.gz
popd

sudo curl -L https://github.com/opencontainers/runc/releases/download/v1.0.0-rc4/runc.amd64 -o /usr/local/bin/runc
sudo chmod +x /usr/local/bin/runc

# this is a lie, but the opencontainer code in P2 depends on this file existing
# to configure runc, so just fudge it for the integration test
sudo echo 'CentOS Linux release 7.4.1708 (Core)' > /etc/redhat-release

sudo env PATH=$PATH GOPATH=$GOPATH GOROOT=$GOROOT go run integration/single-node-slug-deploy/check.go --no-add-user
