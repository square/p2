# Gotcha: Super Simple Assertions for Go

Don't want to have to run a code generator to write test assertions? Sick of dropping boilerplate test runner code in every test file? Gotcha is a super simple Assertion library that just works.

```go
package mymath

import (
	"testing"
	. "github.com/anthonybishopric/gotcha"
)

func TestBasicAddition(t *testing.T) {
	Assert(t).AreEqual(1 + 1, 2, "expected addition to work")
}

```

## Portable Implementation

The `Assert()` function takes an interface `Failer`, shown below:

```go
type Failer interface {
	Fatalf(string, ...interface{})
}

```
So while the `gotcha` package will work with tests, it will also work with instances of `log.Logger`, and can be reused as mainline Assertions.

## Simple Matcher

Flexible matching can be done by implementing the `Matcher` interface

```go
type Matcher (interface{}) bool
```

These can be used together with the `Matches()` function

```go

func ValidPassword(s interface{}) bool {
	 x := s.(string)
	 return len(x) > 6
}

Assert(t).Matches(password, ValidPassword)
```

