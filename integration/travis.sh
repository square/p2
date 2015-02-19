#!/usr/bin/env bash

set -ex

sudo groupadd nobody
sudo useradd hello

sudo /sbin/stop runsvdir
sudo sed -i -e 's;/usr/sbin/runsvdir-start;/usr/bin/runsvdir /var/service;g' /etc/init/runsvdir.conf
sudo /sbin/start runsvdir
sudo mkdir -p /etc/servicebuilder.d /var/service-stage /var/service

sudo cp $GOPATH/bin/p2-exec /usr/local/bin

sudo env PATH=$PATH GOPATH=$GOPATH GOROOT=$GOROOT godep go run integration/single-node-slug-deploy/check.go
