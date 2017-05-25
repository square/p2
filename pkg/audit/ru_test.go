package audit

import (
	"encoding/json"
	"testing"

	"github.com/square/p2/pkg/labels"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	roll_fields "github.com/square/p2/pkg/roll/fields"
	"github.com/square/p2/pkg/types"
	"github.com/square/p2/pkg/util"
)

type fakeLabeler struct {
	labelMap map[string]labels.Labeled
}

func (f fakeLabeler) GetLabels(labelType labels.Type, id string) (labels.Labeled, error) {
	if labelType != labels.RU {
		return labels.Labeled{}, util.Errorf("fakeLabeler is meant for RU labels only")
	}

	return f.labelMap[id], nil
}

func TestRUCompletionEventDetails(t *testing.T) {
	podID := types.PodID("some_pod_id")
	clusterName := pc_fields.ClusterName("some_cluster_name")
	az := pc_fields.AvailabilityZone("some_availability_zone")
	labeler := fakeLabeler{
		labelMap: map[string]labels.Labeled{
			"some_ru": labels.Labeled{
				Labels: map[string]string{
					pc_fields.ClusterNameLabel:      clusterName.String(),
					pc_fields.AvailabilityZoneLabel: az.String(),
					pc_fields.PodIDLabel:            podID.String(),
				},
			},
		},
	}

	ruID := roll_fields.ID("some_ru")

	// normally it can't be canceled and successful at the same time but let's just
	// make sure zero value for bool isn't being used
	detailsJSON, err := NewRUCompletionEventDetails(ruID, true, true, labeler)
	if err != nil {
		t.Fatal(err)
	}

	var details RUCompletionDetails
	err = json.Unmarshal(detailsJSON, &details)
	if err != nil {
		t.Fatal(err)
	}

	if !details.Canceled {
		t.Error("canceled bool didn't get set on details as expected")
	}

	if !details.Succeeded {
		t.Error("succeeded bool didn't get set on details as expected")
	}

	if details.PodID != podID {
		t.Errorf("expected pod id to be %s but was %s", podID, details.PodID)
	}

	if details.AvailabilityZone != az {
		t.Errorf("expected availability zone to be %s but was %s", az, details.AvailabilityZone)
	}

	if details.ClusterName != clusterName {
		t.Errorf("expected cluster name to be %s but was %s", clusterName, details.ClusterName)
	}

	if details.RollingUpdateID != ruID {
		t.Errorf("expected ru ID to be %s but was %s", ruID, details.RollingUpdateID)
	}
}
