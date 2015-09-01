package health

type Result struct {
	ID      string
	Node    string
	Service string
	Status  HealthState
	Output  string
}

type HealthEmpty struct {
	Res string
}

func (h *HealthEmpty) Error() string {
	return h.Res
}

// Returns the poorest status of all checks in the given list, plus the check
// ID of one of those checks.
func FindWorst(results []Result) (string, HealthState, *HealthEmpty) {
	if len(results) == 0 {
		return "", Critical, &HealthEmpty{
			Res: "no results were passed to FindWorst",
		}
	}
	worst, HEErr := FindWorstResult(results)
	if HEErr != nil {
		return "", Critical, HEErr
	}
	return worst.ID, worst.Status, HEErr
}

func FindBest(results []Result) (string, HealthState, *HealthEmpty) {
	if len(results) == 0 {
		return "", Critical, &HealthEmpty{
			Res: "no results were passed to FindBest",
		}
	}
	best, HEErr := FindBestResult(results)
	if HEErr != nil {
		return "", Critical, HEErr
	}
	return best.ID, best.Status, HEErr
}

func FindWorstResult(results []Result) (Result, *HealthEmpty) {
	if len(results) == 0 {
		return Result{Status: Critical}, &HealthEmpty{
			Res: "no results were passed to findWorstResult",
		}
	}
	ret := Passing
	retVal := results[0]
	for _, res := range results {
		if Compare(res.Status, ret) < 0 {
			ret = res.Status
			retVal = res
		}
	}
	return retVal, nil
}

func FindBestResult(results []Result) (Result, *HealthEmpty) {
	if len(results) == 0 {
		return Result{Status: Critical}, &HealthEmpty{
			Res: "no results were passed to findBestResult",
		}
	}
	ret := Critical
	retVal := results[0]
	for _, res := range results {
		if Compare(res.Status, ret) >= 0 {
			ret = res.Status
			retVal = res
		}
	}
	return retVal, nil
}
