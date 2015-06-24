# Functional Tests for P2

This directory contains test scripts that are run against an operating p2 cluster.

Tests have one or more (possibly para-)virtualized machines made accessible with a common setup. This includes:

* Installing the current version of Go
* Setting up important Go variables
* Installing godep and all p2 binaries

## Test layout

```
|──integration
│   ├── common-provision.sh
│   ╰── single-node-slug-deploy
│       ├── check.go
│       ╰── Vagrantfile
```

In this example, `single-node-slug-deploy` is a test case that will execute in the context of the virtual machine spawned by the given Vagrantfile. Consequently, executions are limited to OS X for the time being.

Future versions of this may include the use of the [AWS provider in Vagrant](https://github.com/mitchellh/vagrant-aws) to host many nodes in a single p2 cluster.

## Running the integration tests

1) Install Vagrant
2) If you are using VirtualBox you will likely need the vagrant-vbguest plugin. This can be installed with the following command: vagrant plugin install vagrant-vbguest
3) From the root of the p2 repo run the following command: rake integration
