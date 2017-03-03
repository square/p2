/*
package client implements the normal labels.Applicator functions but maps them
onto a grpc server call
*/
package client

import (
	label_protos "github.com/square/p2/pkg/grpc/labelstore/protos"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type Client struct {
	labelStoreClient label_protos.P2LabelStoreClient
	logger           logging.Logger
}

func NewClient(conn *grpc.ClientConn, logger logging.Logger) Client {
	return Client{
		labelStoreClient: label_protos.NewP2LabelStoreClient(conn),
		logger:           logger,
	}
}

// matches labels.Applicator interface
func (c Client) WatchMatches(selector klabels.Selector, labelType labels.Type, quitCh <-chan struct{}) (chan []labels.Labeled, error) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	go func() {
		<-quitCh

		// Cancel the RPC
		cancelFunc()
	}()
	watchClient, err := c.labelStoreClient.WatchMatches(ctx, &label_protos.WatchMatchesRequest{
		LabelType: labelTypeToProtoLabelType(labelType),
		Selector:  selector.String(),
	})
	if err != nil {
		return nil, err
	}

	outCh := make(chan []labels.Labeled)
	go func() {
		defer close(outCh)
		for {
			labeled, err := watchClient.Recv()
			if grpc.Code(err) == codes.Canceled {
				// This just means quitCh fired and the RPC was canceled as expected
				return
			}

			if err != nil {
				// doesn't hurt to call cancelFunc() more than once potentially
				cancelFunc()
				c.logger.WithError(err).Errorln("Unexpected error reading from WatchMatches stream")
				return
			}

			c.sendOnChannel(outCh, labeled, quitCh)
		}
	}()

	return outCh, nil
}

// Converts a labels.LabelType to the proto label type.
func labelTypeToProtoLabelType(labelType labels.Type) label_protos.LabelType {
	return label_protos.LabelType(label_protos.LabelType_value[labelType.String()])
}

func (c Client) sendOnChannel(outCh chan<- []labels.Labeled, serverResp *label_protos.WatchMatchesResponse, quitCh <-chan struct{}) {
	// need to cast from []*label_protos.Labeled to []labels.Labeled
	ret := make([]labels.Labeled, len(serverResp.Labeled))
	for i, match := range serverResp.Labeled {
		labelType, err := labels.AsType(match.LabelType.String())
		if err != nil {
			// It's potentially really dangerous to omit matches, so we're just going to throw out the whole
			// response. Theoretically this should be impossible
			c.logger.WithError(err).Errorf("Unrecognized label type %s", match.LabelType.String())
			return
		}

		ret[i] = labels.Labeled{
			LabelType: labelType,
			Labels:    match.Labels,
			ID:        match.Id,
		}
	}

	select {
	case outCh <- ret:
	case <-quitCh:
	}
}
