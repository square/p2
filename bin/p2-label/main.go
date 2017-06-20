package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/store/consul/flags"
	"gopkg.in/alecthomas/kingpin.v2"
	klabels "k8s.io/kubernetes/pkg/labels"
)

var (
	cmdApply              = kingpin.Command(CmdApply, "Apply label changes to all objects matching a selector")
	applyAutoConfirm      = cmdApply.Flag("yes", "Autoconfirm label applications. Use with caution!").Short('y').Bool()
	applyLabelType        = cmdApply.Flag("labelType", "The type of label to adjust. Sometimes called the \"label tree\". Supported types can be found here:\n\thttps://godoc.org/github.com/square/p2/pkg/labels#pkg-constants").Short('t').Required().String()
	applySubjectSelector  = cmdApply.Flag("selector", "The selector on which to modify labels. Exclusive with ID").Short('s').String()
	applySubjectID        = cmdApply.Flag("id", "The id of the entry to apply labels to. Exclusive with selector").String()
	applyAddititiveLabels = cmdApply.Flag("add", `The label set to apply to the subject. Include multiple --add switches to include multiple labels. It's safe to mix --add with --delete though the results of this command are not transactional.

Example:
    p2-label --selector $selector --add foo=bar --add bar=baz
`).Short('a').StringMap()
	applyDestructiveLabels = cmdApply.Flag("delete", `The list of label keys to remove from the nodes in the selector. Deletes are idempotent. Include multiple --delete switches to include multiple labels. It's safe to mix --add with --delete though the results of this command are not transactional.

Example:
  p2-label --selector $selector --delete foo --delete bar
`).Short('d').Strings()

	cmdShow       = kingpin.Command(CmdShow, "Show labels that apply to a particular entity (type, ID)")
	showLabelType = cmdShow.Flag("labelType", "The type of label to adjust. Sometimes called the \"label tree\". Supported types can be found here:\n\thttps://godoc.org/github.com/square/p2/pkg/labels#pkg-constants").Short('t').Required().String()
	showID        = cmdShow.Flag("id", "The ID of the entity to show labels for.").Short('i').Required().String()

	// autoConfirm captures the confirmation desire abstractly across commands
	autoConfirm = false
)

const (
	CmdApply = "apply"
	CmdShow  = "show"
)

func main() {
	cmd, _, applicator := flags.ParseWithConsulOptions()
	exitCode := 0

	switch cmd {
	case CmdShow:
		labelType, err := labels.AsType(*showLabelType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while parsing label type. Check the commandline.\n%v\n", err)
			exitCode = 1
			break
		}

		labelsForEntity, err := applicator.GetLabels(labelType, *showID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Got error while querying labels. %v\n", err)
			exitCode = 1
			break
		}
		fmt.Printf("%s/%s: %s\n", labelType, *showID, labelsForEntity.Labels.String())
		return
	case CmdApply:
		// if xnor(selector, id)
		if (*applySubjectSelector == "") == (*applySubjectID == "") {
			fmt.Fprint(os.Stderr, "Must pass either an ID or a selector for objects to apply the given label to")
			exitCode = 1
			break
		}
		autoConfirm = *applyAutoConfirm

		labelType, err := labels.AsType(*applyLabelType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unrecognized type %s. Check the commandline and documentation.\nhttps://godoc.org/github.com/square/p2/pkg/labels#pkg-constants\n", *applyLabelType)
			exitCode = 1
			break
		}

		additiveLabels := *applyAddititiveLabels
		destructiveKeys := *applyDestructiveLabels

		var matches []labels.Labeled
		if *applySubjectSelector != "" {
			subject, err := klabels.Parse(*applySubjectSelector)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while parsing subject label. Check the syntax.\n%v\n", err)
				exitCode = 1
				break
			}

			matches, err = applicator.GetMatches(subject, labelType)
			if err != nil {
				if labels.IsNoLabelsFound(err) {
					fmt.Fprintf(os.Stderr, "No labels were found for the %s type", labelType)
				} else {
					fmt.Fprintf(os.Stderr, "Error while finding label matches. Check the syntax.\n%v\n", err)
				}

				exitCode = 1
				break
			}
		} else {
			matches = []labels.Labeled{{ID: *applySubjectID}}
		}

		if len(additiveLabels) > 0 {
			fmt.Printf("labels to be added: %s\n", klabels.Set(additiveLabels))
		}

		if len(destructiveKeys) > 0 {
			fmt.Printf("labels to be removed: %s\n", destructiveKeys)
		}

		var labelsForEntity labels.Labeled
		for _, match := range matches {
			entityID := match.ID

			err := applyLabels(applicator, entityID, labelType, additiveLabels, destructiveKeys)
			if err != nil {
				fmt.Printf("Encountered err during labeling, %v", err)
				exitCode = 1
			}

			labelsForEntity, err = applicator.GetLabels(labelType, entityID)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Got error while querying labels. %v\n", err)
				exitCode = 1
				continue
			}
			fmt.Printf("%s/%s: %s\n", labelType, entityID, labelsForEntity.Labels.String())
		}
		break
	}

	os.Exit(exitCode)
}

func applyLabels(applicator labels.ApplicatorWithoutWatches, entityID string, labelType labels.Type, additiveLabels map[string]string, destructiveKeys []string) error {
	var err error
	if !confirm(fmt.Sprintf("mutate the labels for %s/%s", labelType, entityID)) {
		return nil
	}
	if len(additiveLabels) > 0 {
		for k, v := range additiveLabels {
			err = applicator.SetLabel(labelType, entityID, k, v)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while appyling label. k/v: %s/%s.\n%v\n", k, v, err)
			}
		}
	}
	if len(destructiveKeys) > 0 {
		for _, key := range destructiveKeys {
			err = applicator.RemoveLabel(labelType, entityID, key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error while destroying label with key: %s.\n%v\n", key, err)
			}
		}
	}
	return nil
}

func confirm(message string) bool {
	if autoConfirm {
		return true
	}

	fmt.Printf("Confirm your intention to %s\n", message)
	fmt.Printf(`Type "y" to confirm [n]: `)
	var input string
	_, err := fmt.Scanln(&input)
	if err != nil {
		return false
	}
	resp := strings.TrimSpace(strings.ToLower(input))
	return resp == "y" || resp == "yes"
}
