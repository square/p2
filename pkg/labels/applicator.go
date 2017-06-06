package labels

import (
	"errors"

	"k8s.io/kubernetes/pkg/labels"
)

// the type of the object being labeled.
type Type string

func (l Type) String() string {
	return string(l)
}

// These types (with the exception of RC) are named such that the path to an
// objects labels is the same as the path to the object but with labelRoot
// prepended.
// Example: the labels for /intent/foo.com/slug are at
// /labels/intent/foo.com/slug.
// Note: the tree name for RCs is pluralized, while the label is not, so this
// doesn't hold true for RCs
const (
	POD  = Type("pod")
	NODE = Type("node")
	PC   = Type("pod_clusters")
	RC   = Type("replication_controller")
	RU   = Type("rolls")
)

var InvalidType error = errors.New("Invalid type provided")

func AsType(v string) (Type, error) {
	switch v {
	case POD.String():
		return POD, nil
	case NODE.String():
		return NODE, nil
	case PC.String():
		return PC, nil
	case RC.String():
		return RC, nil
	case RU.String():
		return RU, nil
	default:
		return Type(""), InvalidType
	}
}

type Labeled struct {
	LabelType Type       `json:"type"`
	ID        string     `json:"id"`
	Labels    labels.Set `json:"labels"`
}

type LabeledChanges struct {
	Created []Labeled
	Updated []Labeled
	Deleted []Labeled
}

func (l Labeled) SameAs(o Labeled) bool {
	return l.ID == o.ID && l.LabelType == o.LabelType
}

// General purpose backing store for labels and assignment
// to P2 objects
type Applicator interface {
	// Assign a label to the object identified by the Type and ID
	SetLabel(labelType Type, id, name, value string) error

	// Assign a group of labels in a batched operation
	SetLabels(labelType Type, id string, labels map[string]string) error

	// Remove a label from the identified object
	RemoveLabel(labelType Type, id, name string) error

	// Remove all labels from the identified object
	RemoveAllLabels(labelType Type, id string) error

	// Lists all labels on all objects associated with this type.
	// mostly useful for secondary caching. Do not use directly
	// if you just want to answer a simple query - use GetMatches
	// instead.
	ListLabels(labelType Type) ([]Labeled, error)

	// Get all Labels assigned to the given object
	GetLabels(labelType Type, id string) (Labeled, error)

	// GetLabelsStale is like GetLabels but may return stale results in
	// exchange for better performance
	GetLabelsStale(labelType Type, id string) (Labeled, error)

	// Return all objects of the given type that match the given selector.
	// When cachedMatch is enabled, Applicators may choose to use an internal cache of
	// aggregated results to answer GetMatches queries.
	GetMatches(selector labels.Selector, labelType Type, cachedMatch bool) ([]Labeled, error)

	// Watch a label selector of a given type and see updates to that set
	// of Labeled over time. If an error occurs, the Applicator implementation
	// is responsible for handling it and recovering gracefully.
	//
	// The watch may be terminated by the underlying implementation without signaling on
	// the quit channel - this will be indicated by the closing of the result channel. For
	// this reason, callers should **always** verify that the channel is closed by checking
	// the "ok" boolean or using `range`.
	WatchMatches(selector labels.Selector, labelType Type, quitCh <-chan struct{}) (chan []Labeled, error)

	// WatchMatchDiff does a diff on top of the results form the WatchMatches and
	// returns a LabeledChanges structure which contain changes
	WatchMatchDiff(
		selector labels.Selector,
		labelType Type,
		quitCh <-chan struct{},
	) <-chan *LabeledChanges
}

// Nothing returns a selector that cannot match any labels. It is different
// from klabels.Nothing() in that it can be serialzed as a string and re-
// parsed as a selector while being in the grammar for selectors.
func Nothing() labels.Selector {
	sel, err := labels.Parse("x=1,x=0")
	if err != nil {
		panic(err)
	}
	return sel
}
