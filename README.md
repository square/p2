# Platypus Platform: Tools for Scalable Software Deployment

[![Build Status](https://travis-ci.org/square/p2.svg?branch=master)](https://travis-ci.org/square/p2)

This is a collection of tools intended to allow huge fleets of machines to participate in safe, flexible and scalable deployment models.

**This project is still under development and should not be used for anything in production yet. We are not seeking external contributors at this time**

# Playing Around

To build the tools in `pp`, run `rake build`. The `bin` subdirectory contains agents and executables, the `pkg` directory contains useful libraries for Go. We strongly believe in small things that do one thing well.

## Layout

* `bin/` contains executables that, together, manage deployment. The `bootstrap` executable can be used to set up new nodes.
* `pkg/` contains standalone libraries that provide supporting functionality of the executables. These libraries are all useful in isolation.

## Dependencies

P2 is based on existing deployment tools at Square. The following list reflects all the system dependencies required by every P2 library, although many libraries require only one of these or are dependency-free.

* [Consul](https://consul.io/)
* [Runit](http://smarden.org/runit/), which includes chpst
* [ServiceBuilder](https://github.com/square/prodeng/tree/master/servicebuilder)
* [Contain](https://github.com/square/prodeng/tree/master/cgroup-container)
* Logrotate
* [Nolimit](https://github.com/square/prodeng/tree/master/nolimit)

# License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)
