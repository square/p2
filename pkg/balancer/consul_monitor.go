package balancer

import (
	"fmt"
	"strings"
	"time"

	"github.com/square/p2/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/square/p2/Godeps/_workspace/src/github.com/hashicorp/consul/api"
	"github.com/square/p2/pkg/health"
	"github.com/square/p2/pkg/logging"
)

type ConsulMonitor struct {
	consul *api.Client
	logger *logging.Logger
}

func NewConsulMonitor(cfg *api.Config, logger *logging.Logger) (Monitor, error) {
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ConsulMonitor{
		client,
		logger,
	}, nil
}

func (c *ConsulMonitor) MonitorHosts(service string, strategy Strategy, quitCh <-chan struct{}) {
	// strip everything after the first . in case the service passed is a vhost
	service = strings.Split(service, ".")[0]

	backoff := time.Duration(5)
	for {
		res, _, err := c.consul.Health().Service(service, "", false, &api.QueryOptions{})
		if err != nil {
			backoff = backoff * 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			c.logger.WithFields(logrus.Fields{
				"err":     err,
				"backoff": backoff,
			}).Warnln("Error when checking status, backing off")
		} else {
			backoff = 5

			for _, serviceEntry := range res {
				healthy := true
				if serviceEntry.Service.Service != service {
					continue
				}
				address := fmt.Sprintf("%s:%d", serviceEntry.Node.Address, serviceEntry.Service.Port)
				for _, check := range serviceEntry.Checks {
					state := health.ToHealthState(check.Status)
					if state == health.Critical || state == health.Unknown {
						healthy = false
						c.logger.WithFields(logrus.Fields{
							"service": check.ServiceName,
							"status":  string(state),
							"address": address,
						}).Warningln("Address is reporting unhealthily")
					}
				}
				if healthy {
					strategy.AddAddress(address)
				} else {
					strategy.RemoveAddress(address)
				}
			}
		}
		select {
		case <-quitCh:
			return
		case <-time.After(backoff * time.Second):

		}
	}
}
