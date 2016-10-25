# Hello app

This directory contains the source for the "hello" pod which is used in the
integration tests. All it does is read the port it should run on from its
config and responds to HTTP requests to '/'. The binary is compiled and built
as a hoist artifact in hoisted-hello_def456.tar.gz so that the integration test
does not need to actually compile anyand built as a hoist artifact in
hoisted-hello_def456.tar.gz so that the integration test does not need to
actually compile anything.

## To update:

* modify the source
* $ go build # on linux 
* $ p2-bin2pod hello # generates a hoist artifact containing the binary
* $ mv <tar_gz_location> ../hoisted-hello_def456.tar.gz
* $ gpg --no-default-keyring --keyring integration/single-node-slug-deploy/pubring.gpg \
  --secret-keyring integration/single-node-slug-deploy/secring.gpg -u p2universe --out \
  integration/hoisted-hello_def456.tar.gz.sig --detach-sign integration/hoisted-hello_def456.tar.gz
* remove the other files
