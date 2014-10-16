package pp

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/square/p2/pkg/logging"

	"testing"
)

func init() {
	logger.SetLogLevel("FATAL")
}

func TestApp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pp-store")
}
