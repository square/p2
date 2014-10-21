/** package gotcha contains a set of common test assertions. It doesn't
attempt to be any sort of fancy test library. Use the Assert() function
to grab an Asserter, which has assertion methods*/
package gotcha

import (
	"fmt"
	"path"
	"runtime"
)

type Failer interface {
	Fatalf(string, ...interface{})
}

type Matcher func(interface{}) bool

type Comparator func(interface{}, interface{}) bool

type Asserter struct {
	t Failer
}

func Assert(t Failer) *Asserter {
	return &Asserter{t}
}

func callerString() string {
	_, filename, line, ok := runtime.Caller(2)
	if !ok {
		return "<unknown>"
	}
	return fmt.Sprintf("at %s:%d", path.Base(filename), line)
}

func (a *Asserter) IsTrue(statement bool, message string) *Asserter {
	if !statement {
		a.t.Fatalf("%s: %s. Was unexpectedly false.", callerString(), message)
	}
	return a
}

func (a *Asserter) IsFalse(statement bool, message string) *Asserter {
	if statement {
		a.t.Fatalf("%s: %s. Was unexpectedly true.", callerString(), message)
	}
	return a
}

func (a *Asserter) AreEqual(left, right interface{}, message string) *Asserter {
	if left != right {
		a.t.Fatalf("%s: %s. Expected %+v to equal %+v.", callerString(), message, left, right)
	}
	return a
}

func (a *Asserter) AreNotEqual(left, right interface{}, message string) *Asserter {
	if left == right {
		a.t.Fatalf("%s: %s. Expected %+v to not equal both arguments", callerString(), message, left, right)
	}
	return a
}

func (a *Asserter) IsNil(subject interface{}, message string) *Asserter {
	if subject != nil {
		a.t.Fatalf("%s: %s. Expected %+v to be nil", callerString(), message, subject)
	}
	return a
}

func (a *Asserter) IsNotNil(subject interface{}, message string) *Asserter {
	if subject == nil {
		a.t.Fatalf("%s: %s. Was unexepectedly nil", callerString(), message)
	}
	return a
}

func (a *Asserter) Fail(message string) {
	a.t.Fatalf("%s: %s", callerString(), message)
}

func (a *Asserter) Matches(subject interface{}, matcher Matcher, message string) {
	if !matcher(subject) {
		a.t.Fatalf("%s: %s", callerString(), message)
	}
}

func (a *Asserter) EachMatch(left []interface{}, right []interface{}, comparator Comparator, message string) {
	if len(left) != len(right) {
		a.t.Fatalf("%s: %s. Left and right don't match in length. (%d, %d)", callerString(), message, len(left), len(right))
	}
	for i, l := range left {
		r := right[i]
		if !comparator(l, r) {
			a.t.Fatalf("%s: %s", callerString(), message)
		}
	}
}

func Equality(l, r interface{}) bool {
	return l == r
}

func Inequality(l, r interface{}) bool {
	return l != r
}
