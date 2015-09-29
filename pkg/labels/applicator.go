package labels

import (
	"errors"
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
	Labels    Set
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

	// Get all Labels assigned to the given object
	GetLabels(labelType Type, id string) (Labeled, error)

	// Return all objects of the given type that match the given selector
	GetMatches(selector Selector, labelType Type) ([]Labeled, error)
}
