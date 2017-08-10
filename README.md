# P2: Tools for Scalable Software Deployment

[![Build Status](https://travis-ci.org/square/p2.svg?branch=master)](https://travis-ci.org/square/p2)

This is a collection of tools intended to allow huge fleets of machines to
participate in safe, flexible and scalable deployment models. It was designed
for Square but is a general-purpose framework that should look _suspiciously
like Kubernetes_ to anyone paying close attention.

## Docker Optional

Using Docker isn't an overnight choice, especially for a company with a long
history of deploying things that aren't Docker. P2 supports our internal
artifact specification ("Hoist artifacts") which are `.tar.gz` files with a
defined layout.

Almost any `.tar.gz` can be a Hoist artifact, as long as it has a `bin/launch`
script or directory of scripts to exec under process management (we use Runit).

Hoist artifacts are totally self-contained and are expected to have all
dependencies statically linked internally with very few exceptions.

P2 executes artifacts in resource constrained cgroups as different users with
different home directories to create _extremely lightweight_ isolation.

## Pods, Labels and Replication Controllers

Kubernetes provides some excellent tools for grouping and managing sets of
applications. We copied them! We didn't want to wait to have our entire Docker
ecosystem established (new build system, new kernel, etc) to start using these
great higher-order orchestration primitives.

We currently have production-quality support for pod manifests, replication controllers
and rolling updates, analagous to Kubernetes pods, replication controllers and deployments,
respectively. We are also actively working on pod clusters, our variation on Kubernetes
services.

## More stuff!

We had to solve a number of problems that Square has today. That led us to the following concepts built-in from the beginning:


* **Arbitrary configuration** files written into the pod manifest, exported and
mounted at `CONFIG_PATH` for applications.
* **Application lifecycle management and health.** During the shutdown of an
instance, we first run `bin/disable`. When starting up an instance, we run
`bin/enable`, and then monitor the application via a call to `GET /_status`. A
200 response code means ready and healthy.
* **Rich plugin architecture for _secret company stuff_**. For example, our
integration with [Keywhiz](https://github.com/square/keywhiz) is implemented in an `after_install` hook. The `hooks`
package in this repo provides a handy Go library for writing hooks that can be
scheduled.
* **Self-hosting!** We wanted to deploy P2 with P2, so we did that. The binary
`p2-bootstrap` allows you to set up a Consul agent and a P2 preparer on the
same host. If done right, that host should allow any future deploys to Just
Work, including to both the Consul agent and the preparer themselves!
* **Deployment Authorization.** From the beginning we needed a way to restrict
who can start which applications. The preparer can be given an ACL that can be
enforced by GPG signatures on pod manifests, signed by the deployer. Or if you
hate GPG, you can use delegated signing with a trusted orchestration service.

# Playing Around

To build the tools in `p2`, run `rake build`. The `bin` subdirectory contains
agents and executables, the `pkg` directory contains useful libraries for Go.
We strongly believe in small things that do one thing well.

## Layout

* `bin/` contains executables that, together, manage deployment. The `bootstrap` executable can be used to set up new nodes.
* `pkg/` contains standalone libraries that provide supporting functionality of the executables. These libraries are all useful in isolation.

## Integration Test

Running `rake integration` will attempt to launch a Vagrant Centos7 machine on
your computer, launch Consul and our preparer and then launch an application.
If you see a success message, you can `vagrant up` the halted box to check out
the setup without needing to do any work yourself.

Ensure that [Vagrant](https://www.vagrantup.com/downloads.html) and
[VirtualBox](https://www.virtualbox.org/wiki/Downloads) are installed if
`rake integration` does not work.

## Dependencies

P2 is based on existing deployment tools at Square. The following list reflects
all the system dependencies required by every P2 library, although many
libraries require only one of these or are dependency-free.

* [Consul](https://consul.io/)
* [Runit](http://smarden.org/runit/), which includes chpst

Many P2 binaries expect to be able to invoke the `p2-exec` binary, ideally by knowing its full path.
The location can be set at compile-time by modifying the `github.com/square/p2/pkg/p2exec.DefaultP2Exec` variable.
The `-X` flag to `go install -ldflags` can be used to perform this assignment.

If the preparer config option `process_result_reporter_config` is set, the preparer will crash unless the configured extractor exists.
We provide one possible implementation at `p2-finish-env-extractor`.

## Desirable Features

Adding Docker support is a big next step, but will ultimately help us migrate to using Docker (or equally excellent RunC implementation) at Square.

P2 also lacks a native job admission / scheduling system, so all pod scheduling is currently done manually by client using either a label selector or simply a hostname. Solutions to this are to be determined.

# License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html)
