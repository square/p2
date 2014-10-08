pp-setline
==========

Ensures the local node is running correct versions of services. Coordinates
with other nodes via distributed leasing if restarts are required to ensure a
minimum number of nodes are always available.

For testing, `fake/bin` provides stubs of required binaries which you can add
to your path.

    PATH=$PATH:./fakebin go run main.go
