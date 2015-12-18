/*
This package implements a log bridge. It is a bridge in the literal sense - it
will carry logs from one side to the other until it reaches its capacity. When
the bridge is at its capacity, it will drop new entries instead of blocking.
This behaviour is desirable because it makes this package safe to attach
to the logging end of an executable

A typical use for this package is to attach it to stdout of an executable
and passing the results into your system's proprietary logging backend.
*/
package logbridge
