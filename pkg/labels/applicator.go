package labels

import (
	"errors"

	"github.com/square/p2/Godeps/_workspace/src/k8s.io/kubernetes/pkg/labels"
)

// the type of the object being labeled.
type Type string

func (l Type) String() string {
	return string(l)
}

const (
	POD  = Type("pod")
	NODE = Type("node")
	RC   = Type("replication_controller")
)

var InvalidType error = errors.New("Invalid type provided")

func AsType(v string) (Type, error) {
	switch v {
	case POD.String():
		return POD, nil
	case NODE.String():
		return NODE, nil
	case RC.String():
		return RC, nil
	default:
		return Type(""), InvalidType
	}
}

type Labeled struct {
	LabelType Type
	ID        string
	Labels    labels.Set
}

func (l Labeled) SameAs(o Labeled) bool {
	return l.ID == o.ID && l.LabelType == o.LabelType
}

// General purpose backing store for labels and assignment
// to P2 objects
type Applicator interface {
	// Assign a label to the object identified by the Type and ID
	SetLabel(labelType Type, id, name, value string) error

	// Remove a label from the identified object
	RemoveLabel(labelType Type, id, name string) error

	// Remove all labels from the identified object
	RemoveAllLabels(labelType Type, id string) error

	// Get all Labels assigned to the given object
	GetLabels(labelType Type, id string) (Labeled, error)

	// Return all objects of the given type that match the given selector
	GetMatches(selector labels.Selector, labelType Type) ([]Labeled, error)

	// Watch a label selector of a given type and see updates to that set
	// of Labeled over time. If an error occurs, the Applicator implementation
	// is responsible for handling it and recovering gracefully.
	//
	// The watch may be terminated by the underlying implementation without signaling on
	// the quit channel - this will be indicated by the closing of the result channel. For
	// this reason, the safest way to process the result of this channel is to use
	// it in a for loop. Alternatively, you may check for nullity of the result []Labeled
	WatchMatches(selector labels.Selector, labelType Type, quitCh chan struct{}) chan *[]Labeled
}
