package labelstore

import (
	label_protos "github.com/square/p2/pkg/grpc/labelstore/protos"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	klabels "k8s.io/kubernetes/pkg/labels"
)

type MatchWatcher interface {
	WatchMatches(selector klabels.Selector, labelType labels.Type, quitCh <-chan struct{}) (chan []labels.Labeled, error)
}

type labelStore struct {
	matchWatcher MatchWatcher
	logger       logging.Logger
}

var _ label_protos.LabelStoreServer = &labelStore{}

func NewServer(matchWatcher MatchWatcher, logger logging.Logger) label_protos.LabelStoreServer {
	return labelStore{
		matchWatcher: matchWatcher,
		logger:       logger,
	}
}

// Streams responses back to the client until cancellation is received via stream.Context().Done()
func (l labelStore) WatchMatches(req *label_protos.WatchMatchesRequest, stream label_protos.LabelStore_WatchMatchesServer) error {
	labelType, err := labels.AsType(req.LabelType.String())
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "Unrecognized label type %s", req.LabelType.String())
	}

	selector, err := klabels.Parse(req.Selector)
	if err != nil {
		return grpc.Errorf(codes.InvalidArgument, "Invalid label selector %s", req.Selector)
	}

	clientCancel := stream.Context().Done()

	quitCh := make(chan struct{})
	defer close(quitCh)
	matchCh, err := l.matchWatcher.WatchMatches(selector, labelType, quitCh)
	if err != nil {
		return err
	}

	for {
		select {
		case <-clientCancel:
			l.logger.Debugln("Request canceled, exiting")
			return nil
		default:
		}

		select {
		case <-clientCancel:
			return nil
		case matches, ok := <-matchCh:
			if ok {
				err = stream.Send(getResponse(matches))
				if err != nil {
					return err
				}
			} else {
				// WatchMatches() can terminate without the quit
				// channel being signaled, just start again
				matchCh, err = l.matchWatcher.WatchMatches(selector, labelType, quitCh)
				if err != nil {
					return err
				}
			}
		}
	}
}

func getResponse(matches []labels.Labeled) *label_protos.WatchMatchesResponse {
	// need to cast from []labels.Labeled to []*label_protos.Labeled
	ret := make([]*label_protos.Labeled, len(matches))
	for i, match := range matches {
		ret[i] = &label_protos.Labeled{
			LabelType: label_protos.LabelType(label_protos.LabelType_value[match.LabelType.String()]),
			Id:        match.ID,
			Labels:    map[string]string(match.Labels),
		}
	}

	return &label_protos.WatchMatchesResponse{
		Labeled: ret,
	}
}
