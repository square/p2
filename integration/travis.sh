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

sudo env PATH=$PATH GOPATH=$GOPATH GOROOT=$GOROOT go run integration/single-node-slug-deploy/check.go
