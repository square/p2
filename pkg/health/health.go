package health

// Result stores the health state of a service.
type Result struct {
	ID      string
	Node    string
	Service string
	Status  HealthState
	Output  string
}

// ResultList is a type alias that adds some extra methods that operate on the list.
type ResultList []Result

// MaxValue returns a pointer to a Result with the best health status in the list. If the
// list is empty, returns nil.
func (l ResultList) MaxValue() *Result {
	if len(l) == 0 {
		return nil
	}
	max := 0
	for i := range l[1:] {
		if Compare(l[i+1].Status, l[max].Status) > 0 {
			max = i + 1
		}
	}
	return &l[max]
}

// MaxResult is a shortcult that builds a ResultList from the inputs and calls
// ResultList.MaxValue() on it.
func MaxResult(r1 Result, rest ...Result) Result {
	l := make(ResultList, 1, 1+len(rest))
	l[0] = r1
	l = append(l, rest...)
	return *l.MaxValue()
}

// MinValue returns a pointer to a Result with the worst health status in the list. If the
// list is empty, returns nil.
func (l ResultList) MinValue() *Result {
	if len(l) == 0 {
		return nil
	}
	min := 0
	for i := range l[1:] {
		if Compare(l[i+1].Status, l[min].Status) < 0 {
			min = i + 1
		}
	}
	return &l[min]
}

// MinResult is a shortcut that builds a ResultList from the inputs and calls
// ResultList.MinValue() on it.
func MinResult(r1 Result, rest ...Result) Result {
	l := make(ResultList, 1, 1+len(rest))
	l[0] = r1
	l = append(l, rest...)
	return *l.MinValue()
}
